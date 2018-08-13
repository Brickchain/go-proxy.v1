package pubsub

import "time"

type PubSubInterface interface {
	Publish(topic string, data string) error
	Subscribe(group string, topic string) (Subscriber, error)
	DeleteTopic(topic string) error
	Stop()
}

type Subscriber interface {
	Pull(timeout time.Duration) (string, int)
	Stop(timeout time.Duration)
	Chan() chan string
}
