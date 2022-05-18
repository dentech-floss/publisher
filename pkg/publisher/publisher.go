package messaging

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"

	"github.com/garsue/watermillzap"

	wotelfloss "github.com/dentech-floss/watermill-opentelemetry-go-extra/pkg/opentelemetry"
	wotel "github.com/voi-oss/watermill-opentelemetry/pkg/opentelemetry"

	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"

	"go.uber.org/zap"
)

var defaultPublisherName = "pubsub.Publish"
var defaultConnectTimeoutSecs = 20
var defaultPublishTimeoutSecs = 10
var defaultGrpcConnectionPool = 10

type PublisherConfig struct {
	OnGCP                  bool
	ProjectId              string  // only applicable if OnGCP is true
	Name                   *string // defaults to "pubsub.Publish" if not set
	ConnectTimeoutSecs     *int
	PublishTimeoutSecs     *int
	GrpcConnectionPoolSize *int
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
}

type Publisher struct {
	message.Publisher
}

func NewPublisher(
	logger *zap.Logger,
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
			watermillzap.NewLogger(logger),
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

	return &Publisher{wotelPublisherDecorator}
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