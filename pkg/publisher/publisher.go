package publisher

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"

	"github.com/dentech-floss/logging/pkg/logging"
	wotelfloss "github.com/dentech-floss/watermill-opentelemetry-go-extra/pkg/opentelemetry"
	wotel "github.com/voi-oss/watermill-opentelemetry/pkg/opentelemetry"

	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

var (
	defaultPublisherName      = "pubsub.Publish"
	defaultConnectTimeoutSecs = 20
	defaultPublishTimeoutSecs = 10
	defaultGrpcConnectionPool = 10
)

type PublisherConfig struct {
	OnGCP                  bool
	ProjectId              string  // only applicable if OnGCP is true
	Name                   *string // defaults to "pubsub.Publish" if not set
	ConnectTimeoutSecs     *int
	PublishTimeoutSecs     *int
	GrpcConnectionPoolSize *int
	RetryConfig            *PublisherRetryConfig
}

func (c *PublisherConfig) setDefaults() {
	if c.Name == nil {
		c.Name = &defaultPublisherName
	}
	if c.ConnectTimeoutSecs == nil {
		c.ConnectTimeoutSecs = &defaultConnectTimeoutSecs
	}
	if c.PublishTimeoutSecs == nil {
		c.PublishTimeoutSecs = &defaultPublishTimeoutSecs
	}
	if c.GrpcConnectionPoolSize == nil {
		c.GrpcConnectionPoolSize = &defaultGrpcConnectionPool
	}
	if c.RetryConfig == nil {
		c.RetryConfig = &PublisherRetryConfig{}
	}
	c.RetryConfig.setDefaults()
}

var (
	defaultMaxRetries          = 10
	defaultInitialInterval     = 500 * time.Millisecond
	defaultRandomizationFactor = 0.5
	defaultMultiplier          = 1.5
	defaultMaxInterval         = 60 * time.Second
	defaultMaxElapsedTime      = 15 * time.Minute
)

type PublisherRetryConfig struct {
	MaxRetries          *int
	InitialInterval     *time.Duration
	RandomizationFactor *float64
	Multiplier          *float64
	MaxInterval         *time.Duration
	MaxElapsedTime      *time.Duration
}

func (c *PublisherRetryConfig) setDefaults() {
	if c.MaxRetries == nil {
		c.MaxRetries = &defaultMaxRetries
	}
	if c.InitialInterval == nil {
		c.InitialInterval = &defaultInitialInterval
	}
	if c.RandomizationFactor == nil {
		c.RandomizationFactor = &defaultRandomizationFactor
	}
	if c.Multiplier == nil {
		c.Multiplier = &defaultMultiplier
	}
	if c.MaxInterval == nil {
		c.MaxInterval = &defaultMaxInterval
	}
	if c.MaxElapsedTime == nil {
		c.MaxElapsedTime = &defaultMaxElapsedTime
	}
}

type Publisher struct {
	Publisher message.Publisher
	Retry     *middleware.Retry
}

func NewPublisher(
	logger *logging.Logger,
	config *PublisherConfig,
) *Publisher {
	config.setDefaults()

	var publisher message.Publisher
	var err error

	if config.OnGCP {
		publisher, err = googlecloud.NewPublisher(
			googlecloud.PublisherConfig{
				ProjectID:      config.ProjectId,
				ConnectTimeout: time.Duration(*config.ConnectTimeoutSecs) * time.Second,
				PublishTimeout: time.Duration(*config.PublishTimeoutSecs) * time.Second,
				ClientOptions: []option.ClientOption{
					option.WithGRPCConnectionPool(*config.GrpcConnectionPoolSize),
				},
			},
			logging.NewWatermillAdapter(logger),
		)
	} else {
		publisher = NewFakePublisher()
	}
	if err != nil {
		panic(err)
	}

	// Now it get's interesting... the Opentelemetry decorator from the Watermill
	// community creates a span when publishing message(s) so that't fine and dandy
	// BUT the trace/span id is not included in the message... which means that we
	// can't extract the trace/span in the subscriber(s) and create child spans there.
	// So this is taken care of here: we first let the Watermill/Opentelmetry decorator
	// create a span (the context of the first message will contain this) and then we
	// extract the trace/span id in a second custom decorator that add the trace/span id's
	// as metadata on the message before delegating to the actual googlecloud publisher.

	tracePropagatingPublisherDecorator := wotelfloss.NewTracePropagatingPublisherDecorator(publisher)
	wotelPublisherDecorator := wotel.NewNamedPublisherDecorator(*config.Name, tracePropagatingPublisherDecorator)

	return &Publisher{
		Publisher: wotelPublisherDecorator,
		Retry:     createRetry(logger, config.RetryConfig),
	}
}

func createRetry(
	logger *logging.Logger,
	config *PublisherRetryConfig,
) *middleware.Retry {
	return &middleware.Retry{
		MaxRetries:          *config.MaxRetries,
		InitialInterval:     *config.InitialInterval,
		RandomizationFactor: *config.RandomizationFactor,
		Multiplier:          *config.Multiplier,
		MaxInterval:         *config.MaxInterval,
		MaxElapsedTime:      *config.MaxElapsedTime,
		Logger:              logging.NewWatermillAdapter(logger),
	}
}

func (p *Publisher) NewMessage(
	ctx context.Context,
	payload proto.Message,
) (*message.Message, error) {
	bytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, err
	}
	msg := message.NewMessage(watermill.NewUUID(), bytes)
	msg.SetContext(ctx) // for tracing...

	return msg, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if p.Retry == nil {
		return p.Publisher.Publish(topic, messages...)
	} else {
		h := p.Retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
			err := p.Publisher.Publish(topic, msg)
			return nil, err
		})
		for _, msg := range messages {
			_, err := h(msg)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func (p *Publisher) Close() error {
	return p.Publisher.Close()
}
