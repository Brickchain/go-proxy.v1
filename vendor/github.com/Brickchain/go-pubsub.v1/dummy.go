package pubsub

import "time"

type DummyPubSub struct{}

func NewDummyPubSub() *DummyPubSub {
	return &DummyPubSub{}
}

func (d *DummyPubSub) Stop() {
	return
}

func (d *DummyPubSub) Publish(topic string, data string) error {

	return nil
}

func (d *DummyPubSub) Subscribe(group, topic string) (Subscriber, error) {
	s := DummySubscriber{}

	return &s, nil
}

func (d *DummyPubSub) DeleteTopic(topic string) error {
	return nil
}

type DummySubscriber struct{}

func (s *DummySubscriber) Pull(timeout time.Duration) (string, int) {
	return "test", 0
}

func (s *DummySubscriber) Chan() chan string {
	c := make(chan string, 1)
	c <- "test"
	return c
}

func (s *DummySubscriber) Stop(timeout time.Duration) {

}
