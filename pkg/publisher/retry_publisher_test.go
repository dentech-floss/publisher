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

func Test_PublishWithRetry(t *testing.T) {

	maxRetries := 3

	failingPublisher := failingPublisher{runCount: maxRetries}

	config := RetryPublisherConfig{}
	config.setDefaults()

	retryPublisher := &RetryPublisher{&failingPublisher, createRetry(zap.NewNop(), &config)}

	err := retryPublisher.Publish("topic", message.NewMessage(watermill.NewUUID(), []byte("test")))
	if err != nil {
		t.Fatal(err)
	}

	if failingPublisher.runCount != 0 {
		t.Error("should be zero")
	}
}
