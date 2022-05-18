package messaging

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

type FakePublishedEntry struct {
	Topic    string
	Messages []*message.Message
}

type FakePublisher struct {
	published []*FakePublishedEntry
}

func NewFakePublisher() message.Publisher {
	return &FakePublisher{
		published: make([]*FakePublishedEntry, 0),
	}
}

func (p *FakePublisher) Publish(topic string, messages ...*message.Message) error {
	p.published = append(
		p.published,
		&FakePublishedEntry{
			Topic:    topic,
			Messages: messages,
		},
	)
	return nil
}

func (p *FakePublisher) Close() error {
	return nil
}

// For testing purposes...

func (p *FakePublisher) GetPublished() []*FakePublishedEntry {
	return p.published
}

func (p *FakePublisher) ClearPublished() {
	p.published = make([]*FakePublishedEntry, 0)
}
