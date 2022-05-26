package publisher

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/garsue/watermillzap"

	"go.uber.org/zap"
)

var defaultMaxRetries = 10
var defaultInitialInterval = 500 * time.Millisecond
var defaultRandomizationFactor = 0.5
var defaultMultiplier = 1.5
var defaultMaxInterval = 60 * time.Second
var defaultMaxElapsedTime = 15 * time.Minute

type RetryPublisherConfig struct {
	PublisherConfig     *PublisherConfig
	MaxRetries          *int
	InitialInterval     *time.Duration
	RandomizationFactor *float64
	Multiplier          *float64
	MaxInterval         *time.Duration
	MaxElapsedTime      *time.Duration
}

func (c *RetryPublisherConfig) setDefaults() {
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

type RetryPublisher struct {
	publisher message.Publisher
	retry     *middleware.Retry
}

func NewRetryPublisher(
	logger *zap.Logger,
	config *RetryPublisherConfig,
) *RetryPublisher {
	config.setDefaults()
	return &RetryPublisher{
		publisher: NewPublisher(logger, config.PublisherConfig),
		retry:     createRetry(logger, config),
	}
}

func createRetry(
	logger *zap.Logger,
	config *RetryPublisherConfig,
) *middleware.Retry {
	return &middleware.Retry{
		MaxRetries:          *config.MaxRetries,
		InitialInterval:     *config.InitialInterval,
		RandomizationFactor: *config.RandomizationFactor,
		Multiplier:          *config.Multiplier,
		MaxInterval:         *config.MaxInterval,
		MaxElapsedTime:      *config.MaxElapsedTime,
		Logger:              watermillzap.NewLogger(logger),
	}
}

func (p *RetryPublisher) Publish(topic string, messages ...*message.Message) error {
	h := p.retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		err := p.publisher.Publish(topic, msg)
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

func (p *RetryPublisher) Close() error {
	return p.publisher.Close()
}
