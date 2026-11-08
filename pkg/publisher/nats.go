package publisher

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"unicode"

	wmjs "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dentech-floss/logging/pkg/logging"
	"github.com/nats-io/nats.go"
	"go.yaml.in/yaml/v3"
)

// NATSConfig is how the publisher connects to NATS and maps topics to
// subjects. Every empty field is read from the environment variable named
// in its comment.
type NATSConfig struct {
	// URL of the NATS server, e.g. tls://host:4222. NATS_URL.
	URL string
	// CAFile is a PEM file with the CA that signed the server's
	// certificate. Optional: without it, the system roots are used.
	// NATS_CA_FILE.
	CAFile string
	// User and Password log in with a user from the server's config.
	// NATS_USER, NATS_PASSWORD.
	User     string
	Password string
	// CredsFile logs in with a JWT and NKey seed (operator mode) instead
	// of User and Password. NATS_CREDS_FILE.
	CredsFile string
	// MappingFile is the YAML file that maps topics to NATS subjects.
	// NATS_MAPPING_FILE.
	MappingFile string
}

func (c *NATSConfig) setDefaults() {
	fromEnv := func(field *string, name string) {
		if *field == "" {
			*field = os.Getenv(name)
		}
	}
	fromEnv(&c.URL, "NATS_URL")
	fromEnv(&c.CAFile, "NATS_CA_FILE")
	fromEnv(&c.User, "NATS_USER")
	fromEnv(&c.Password, "NATS_PASSWORD")
	fromEnv(&c.CredsFile, "NATS_CREDS_FILE")
	fromEnv(&c.MappingFile, "NATS_MAPPING_FILE")
}

func (c *NATSConfig) validate() error {
	var errs []error
	if c.URL == "" {
		errs = append(errs, errors.New("NATS_URL is not set"))
	}
	if c.MappingFile == "" {
		errs = append(errs, errors.New("NATS_MAPPING_FILE is not set"))
	}
	if c.CredsFile != "" && c.User != "" {
		errs = append(errs, errors.New("set either NATS_CREDS_FILE or NATS_USER, not both"))
	}
	if c.User != "" && c.Password == "" {
		errs = append(errs, errors.New("NATS_USER is set but NATS_PASSWORD is empty"))
	}
	return errors.Join(errs...)
}

// connect opens a connection that reconnects forever: a NATS restart must
// not stop the service.
func (c *NATSConfig) connect(name string) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
	}
	if c.CAFile != "" {
		opts = append(opts, nats.RootCAs(c.CAFile))
	}
	if c.User != "" {
		opts = append(opts, nats.UserInfo(c.User, c.Password))
	}
	if c.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(c.CredsFile))
	}
	nc, err := nats.Connect(c.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", c.URL, err)
	}
	return nc, nil
}

// natsMapping is the part of the mapping file that the publisher reads. The
// same file holds the subscribers' consumers and the stream settings for
// the infrastructure code.
type natsMapping struct {
	Topics map[string]natsTopic `yaml:"topics"`
}

// natsTopic says where messages published to one topic go. Exactly one of:
//   - Skip: not published at all (e.g. a legacy topic whose events are also
//     published to another topic).
//   - Subject: every message goes to this subject.
//   - Attribute: the message's metadata value for this key picks the
//     subject from Subjects. A missing or unknown value goes to
//     <Domain>.unmapped.<value>.
type natsTopic struct {
	Skip      bool              `yaml:"skip"`
	Subject   string            `yaml:"subject"`
	Attribute string            `yaml:"attribute"`
	Domain    string            `yaml:"domain"`
	Subjects  map[string]string `yaml:"subjects"`
}

func loadNATSMapping(path string) (*natsMapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading the NATS mapping: %w", err)
	}
	var m natsMapping
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parsing the NATS mapping %s: %w", path, err)
	}
	for name, t := range m.Topics {
		kinds := 0
		if t.Skip {
			kinds++
		}
		if t.Subject != "" {
			kinds++
		}
		if t.Attribute != "" {
			kinds++
			if t.Domain == "" {
				return nil, fmt.Errorf("NATS mapping %s: topic %s has an attribute but no domain for the fallback", path, name)
			}
		}
		if kinds != 1 {
			return nil, fmt.Errorf("NATS mapping %s: topic %s needs exactly one of skip, subject or attribute", path, name)
		}
		subjects := map[string]string{"subject": t.Subject, "domain": t.Domain}
		for value, s := range t.Subjects {
			subjects["subjects."+value] = s
		}
		for field, s := range subjects {
			if s == "" && (field == "subject" || field == "domain") {
				continue
			}
			if err := validSubject(s); err != nil {
				return nil, fmt.Errorf("NATS mapping %s: topic %s, %s: %w", path, name, field, err)
			}
		}
	}
	return &m, nil
}

// validSubject accepts a subject that can be published to: dot-separated,
// non-empty tokens without whitespace or wildcards.
func validSubject(s string) error {
	for _, token := range strings.Split(s, ".") {
		if token == "" {
			return fmt.Errorf("subject %q has an empty token", s)
		}
		if strings.IndexFunc(token, unicode.IsSpace) >= 0 || strings.ContainsAny(token, "*>") {
			return fmt.Errorf("subject %q has whitespace or a wildcard", s)
		}
	}
	return nil
}

// subjectFor returns the subject for a message published to topic. skip is
// true if the topic isn't published. fallback says why the message went to
// an unmapped subject, or is empty.
func (m *natsMapping) subjectFor(topic string, md message.Metadata) (subject string, skip bool, fallback string) {
	t, ok := m.Topics[topic]
	switch {
	case !ok:
		return "unmapped." + subjectToken(topic), false, reasonUnmappedTopic
	case t.Skip:
		return "", true, ""
	case t.Subject != "":
		return t.Subject, false, ""
	}
	value := md.Get(t.Attribute)
	if s, ok := t.Subjects[value]; ok && value != "" {
		return s, false, ""
	}
	if value == "" {
		return t.Domain + ".unmapped.none", false, "message has no " + t.Attribute
	}
	return t.Domain + ".unmapped." + subjectToken(value), false, t.Attribute + " not in the mapping"
}

// MetadataUnmappedTopic is set on messages published to unmapped.<topic>:
// the topic as the service published it.
const MetadataUnmappedTopic = "unmapped_topic"

const reasonUnmappedTopic = "topic not in the mapping"

var nonTokenChars = regexp.MustCompile(`[^a-z0-9_-]+`)

// subjectToken turns any string into one lower-case subject token.
func subjectToken(s string) string {
	t := nonTokenChars.ReplaceAllString(strings.ToLower(s), "_")
	if t == "" {
		return "none"
	}
	return t
}

// natsPublisher publishes each message to the subject the mapping gives
// for its topic and metadata, with JetStream (the watermill-nats JetStream
// publisher). The message UUID is the Nats-Msg-Id, so a retried publish
// isn't stored twice.
type natsPublisher struct {
	logger  *logging.Logger
	mapping *natsMapping
	inner   *wmjs.Publisher
}

func newNATSPublisher(logger *logging.Logger, name string, config NATSConfig) (*natsPublisher, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	mapping, err := loadNATSMapping(config.MappingFile)
	if err != nil {
		return nil, err
	}
	nc, err := config.connect(name)
	if err != nil {
		return nil, err
	}
	inner, err := wmjs.NewPublisher(wmjs.PublisherConfig{
		Conn:           nc,
		Logger:         logging.NewWatermillAdapter(logger),
		TrackMessageID: true,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &natsPublisher{logger: logger, mapping: mapping, inner: inner}, nil
}

func (p *natsPublisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		subject, skip, fallback := p.mapping.subjectFor(topic, msg.Metadata)
		if skip {
			continue
		}
		if fallback != "" {
			if fallback == reasonUnmappedTopic {
				// unmapped.<topic> is lossy (lower case, _), so two topics can
				// share it: keep the topic itself in a header. On a copy, so
				// the caller's message doesn't carry it to a later publish.
				msg = msg.CopyWithContext()
				msg.Metadata.Set(MetadataUnmappedTopic, topic)
			}
			p.logger.Warn("Published to an unmapped NATS subject: add it to the NATS mapping",
				logging.String("topic", topic),
				logging.String("subject", subject),
				logging.String("reason", fallback),
				logging.String("msg_uuid", msg.UUID),
			)
		}
		if err := p.inner.Publish(subject, msg); err != nil {
			return fmt.Errorf("publishing %s to NATS subject %s: %w", topic, subject, err)
		}
	}
	return nil
}

func (p *natsPublisher) Close() error {
	return p.inner.Close()
}
