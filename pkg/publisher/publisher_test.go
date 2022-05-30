package publisher

import (
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"go.uber.org/zap"
)

type failingPublisher struct {
	runCount int
}

func (f *failingPublisher) Publish(topic string, messages ...*message.Message) error {
	f.runCount--
	if f.runCount > 0 {
		return errors.New("failing")
	}
	return nil
}

func (f *failingPublisher) Close() error {
	return nil
}

func Test_Publish_Retry(t *testing.T) {

	maxRetries := 3

	failingPublisher := failingPublisher{runCount: maxRetries}

	publisher := NewPublisher(
		zap.NewNop(),
		&PublisherConfig{
			OnGCP:     false,
			ProjectId: "",
			RetryConfig: &PublisherRetryConfig{
				MaxRetries: &maxRetries,
			},
		},
	)

	// swap out the underlying watermill publisher for our failing publisher
	publisher.Publisher = &failingPublisher

	err := publisher.Publish("topic", message.NewMessage(watermill.NewUUID(), []byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	if failingPublisher.runCount != 0 {
		t.Error("should be zero")
	}
}
