package publisher

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	wmnats "github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dentech-floss/logging/pkg/logging"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const testMapping = `
topics:
  thing.created:
    subject: things.thing.created
  thing:
    attribute: event_type
    domain: things
    subjects:
      EVENT_TYPE_UPDATED: things.thing.updated
      EVENT_TYPE_DELETED: things.thing.deleted
  thing.legacy_updated:
    skip: true
consumers: # for the subscriber, ignored here
  - service: example
    url: /push-handlers/pubsub/thing
    stream: THINGS
    durable: example_thing
`

type natsEnv struct {
	url     string
	mapping string
	js      jetstream.JetStream
}

// setupNATS starts a JetStream server with a stream for the things domain
// and one for unmapped topics, and writes the mapping file.
func setupNATS(t *testing.T) natsEnv {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	for name, subject := range map[string]string{"THINGS": "things.>", "UNMAPPED": "unmapped.>"} {
		if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
			Name:     name,
			Subjects: []string{subject},
		}); err != nil {
			t.Fatal(err)
		}
	}
	mapping := filepath.Join(t.TempDir(), "mapping.yaml")
	writeFile(t, mapping, testMapping)
	return natsEnv{url: srv.ClientURL(), mapping: mapping, js: js}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (e natsEnv) publisher(t *testing.T) *Publisher {
	t.Helper()
	p := NewPublisher(
		logging.NewLogger(&logging.LoggerConfig{ServiceName: "test"}),
		&PublisherConfig{
			Broker: BrokerNATS,
			NATS:   NATSConfig{URL: e.url, MappingFile: e.mapping},
		},
	)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// last returns the newest message on subject, or nil.
func (e natsEnv) last(t *testing.T, stream, subject string) *jetstream.RawStreamMsg {
	t.Helper()
	s, err := e.js.Stream(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.GetLastMsgForSubject(context.Background(), subject)
	if err != nil {
		return nil
	}
	return m
}

func (e natsEnv) count(t *testing.T, stream string) uint64 {
	t.Helper()
	s, err := e.js.Stream(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func newMsg(uuid string, metadata map[string]string) *message.Message {
	msg := message.NewMessage(uuid, []byte("payload-"+uuid))
	for k, v := range metadata {
		msg.Metadata.Set(k, v)
	}
	return msg
}

func TestNATSSubjects(t *testing.T) {
	env := setupNATS(t)
	p := env.publisher(t)

	for _, tc := range []struct {
		topic, eventType, stream, subject string
	}{
		{"thing.created", "", "THINGS", "things.thing.created"},
		{"thing", "EVENT_TYPE_UPDATED", "THINGS", "things.thing.updated"},
		{"thing", "EVENT_TYPE_DELETED", "THINGS", "things.thing.deleted"},
		{"thing", "EVENT_TYPE_Brand New", "THINGS", "things.unmapped.event_type_brand_new"},
		{"thing", "", "THINGS", "things.unmapped.none"},
		{"other.Topic", "", "UNMAPPED", "unmapped.other_topic"},
	} {
		t.Run(tc.topic+"/"+tc.eventType, func(t *testing.T) {
			uuid := strings.NewReplacer(".", "-", "/", "-", " ", "-").Replace(tc.subject)
			md := map[string]string{"trace_id": "0af7651916cd43dd8448eb211c80319c"}
			if tc.eventType != "" {
				md["event_type"] = tc.eventType
			}
			if err := p.Publish(tc.topic, newMsg(uuid, md)); err != nil {
				t.Fatal(err)
			}
			got := env.last(t, tc.stream, tc.subject)
			if got == nil {
				t.Fatalf("nothing on %s", tc.subject)
			}
			if string(got.Data) != "payload-"+uuid {
				t.Errorf("payload %q", got.Data)
			}
			for k, want := range map[string]string{
				wmnats.WatermillUUIDHdr: uuid,
				nats.MsgIdHdr:           uuid,
				"event_type":            tc.eventType,
				"trace_id":              "0af7651916cd43dd8448eb211c80319c",
			} {
				if v := got.Header.Get(k); v != want {
					t.Errorf("header %s = %q, want %q", k, v, want)
				}
			}
		})
	}
}

func TestNATSSkippedTopic(t *testing.T) {
	env := setupNATS(t)
	p := env.publisher(t)
	before := env.count(t, "THINGS") + env.count(t, "UNMAPPED")
	if err := p.Publish("thing.legacy_updated", newMsg("skipped", nil)); err != nil {
		t.Fatal(err)
	}
	if after := env.count(t, "THINGS") + env.count(t, "UNMAPPED"); after != before {
		t.Errorf("skipped topic was published: %d -> %d messages", before, after)
	}
}

func TestNATSRetriedPublishStoredOnce(t *testing.T) {
	env := setupNATS(t)
	p := env.publisher(t)
	msg := newMsg("same-uuid", nil)
	for range 2 {
		if err := p.Publish("thing.created", msg); err != nil {
			t.Fatal(err)
		}
	}
	if n := env.count(t, "THINGS"); n != 1 {
		t.Errorf("%d messages stored, want 1 (Nats-Msg-Id dedup)", n)
	}
}

func TestNATSNoStreamIsAnError(t *testing.T) {
	env := setupNATS(t)
	writeFile(t, env.mapping, "topics:\n  thing.created:\n    subject: nowhere.thing.created\n")
	maxRetries := 1
	maxElapsed := time.Second
	p := NewPublisher(
		logging.NewLogger(&logging.LoggerConfig{ServiceName: "test"}),
		&PublisherConfig{
			Broker:      BrokerNATS,
			NATS:        NATSConfig{URL: env.url, MappingFile: env.mapping},
			RetryConfig: &PublisherRetryConfig{MaxRetries: &maxRetries, MaxElapsedTime: &maxElapsed},
		},
	)
	defer p.Close()
	if err := p.Publish("thing.created", newMsg("lost", nil)); err == nil {
		t.Fatal("no error for a subject no stream captures")
	}
}

func TestNATSConfigFromEnvironment(t *testing.T) {
	env := setupNATS(t)
	t.Setenv("BROKER", "nats")
	t.Setenv("NATS_URL", env.url)
	t.Setenv("NATS_MAPPING_FILE", env.mapping)
	p := NewPublisher(logging.NewLogger(&logging.LoggerConfig{ServiceName: "test"}), &PublisherConfig{})
	defer p.Close()
	if err := p.Publish("thing.created", newMsg("from-env", nil)); err != nil {
		t.Fatal(err)
	}
	if env.last(t, "THINGS", "things.thing.created") == nil {
		t.Fatal("not published")
	}
}

func TestBrokerUnsetKeepsOnGCPBehaviour(t *testing.T) {
	t.Setenv("BROKER", "")
	p := NewPublisher(logging.NewLogger(&logging.LoggerConfig{}), &PublisherConfig{OnGCP: false})
	if err := p.Publish("thing.created", newMsg("fake", nil)); err != nil {
		t.Fatal(err)
	}
	// The trace decorators wrap the fake, so check through Close only:
	// no NATS or Pub/Sub connection was needed.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNATSConfigErrorsPanic(t *testing.T) {
	env := setupNATS(t)
	bad := func(content string) string {
		path := filepath.Join(t.TempDir(), "bad.yaml")
		writeFile(t, path, content)
		return path
	}
	for name, tc := range map[string]struct {
		config PublisherConfig
		want   string
	}{
		"unknown broker":      {PublisherConfig{Broker: "kafka"}, "unknown broker"},
		"no URL":              {PublisherConfig{Broker: BrokerNATS, NATS: NATSConfig{MappingFile: env.mapping}}, "NATS_URL"},
		"no mapping":          {PublisherConfig{Broker: BrokerNATS, NATS: NATSConfig{URL: env.url}}, "NATS_MAPPING_FILE"},
		"user and creds":      {PublisherConfig{Broker: BrokerNATS, NATS: NATSConfig{URL: env.url, MappingFile: env.mapping, User: "u", Password: "p", CredsFile: "/c"}}, "not both"},
		"two kinds":           {PublisherConfig{Broker: BrokerNATS, NATS: NATSConfig{URL: env.url, MappingFile: bad("topics:\n  a:\n    skip: true\n    subject: x.y.z\n")}}, "exactly one of"},
		"attribute no domain": {PublisherConfig{Broker: BrokerNATS, NATS: NATSConfig{URL: env.url, MappingFile: bad("topics:\n  a:\n    attribute: event_type\n")}}, "no domain"},
		"server down":         {PublisherConfig{Broker: BrokerNATS, NATS: NATSConfig{URL: "nats://127.0.0.1:1", MappingFile: env.mapping}}, "connecting to NATS"},
	} {
		t.Run(name, func(t *testing.T) {
			for _, v := range []string{"BROKER", "NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CREDS_FILE", "NATS_MAPPING_FILE", "NATS_CA_FILE"} {
				t.Setenv(v, "")
			}
			defer func() {
				r := recover()
				if r == nil || !strings.Contains(r.(string), tc.want) {
					t.Fatalf("panic %v, want one mentioning %q", r, tc.want)
				}
			}()
			config := tc.config
			NewPublisher(logging.NewLogger(&logging.LoggerConfig{}), &config)
		})
	}
}
