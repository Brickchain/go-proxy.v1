package pubsub

import (
	"fmt"
	"time"

	logger "github.com/Brickchain/go-logger.v1"
	"github.com/go-redis/redis"
	"golang.org/x/net/context"
)

type RedisPubSub struct {
	client *redis.Client
	addr   string
}

func NewRedisPubSub(addr string) (*RedisPubSub, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    "", // no password set
		DB:          0,  // use default DB
		IdleTimeout: time.Second * 10,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	r := RedisPubSub{
		client: client,
		addr:   addr,
	}

	return &r, nil
}

func (r *RedisPubSub) Stop() {
	r.client.Close()
}

func (r *RedisPubSub) Publish(topic string, data string) error {
	res := r.client.Publish(topic, data)
	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

func (r *RedisPubSub) DeleteTopic(topicName string) error {

	return nil
}

type RedisSubscriber struct {
	client  *redis.Client
	topic   string
	sub     *redis.PubSub
	output  chan string
	ctx     context.Context
	cancel  func()
	ready   chan bool
	running bool
}

func (r *RedisPubSub) Subscribe(group, topic string) (Subscriber, error) {
	s := RedisSubscriber{
		client: r.client,
		topic:  topic,
		output: make(chan string, 100),
		ready:  make(chan bool),
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	go s.run()

	// wait for subscriber to tell us it's ready
	ok := <-s.ready
	if !ok {
		return nil, fmt.Errorf("Something went wrong while setting up the subscriber")
	}

	return &s, nil
}

func (s *RedisSubscriber) run() {
	s.running = true
	defer func() {
		s.running = false
	}()

	s.sub = s.client.Subscribe(s.topic)

	_, err := s.sub.Receive()
	if err != nil {
		logger.Error(err)
		s.ready <- false
		return
	}

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				close(s.output)
				return
			case m := <-s.sub.Channel():
				if m != nil && s.output != nil {
					s.output <- m.Payload
				}
			}
		}
	}()

	s.ready <- true
}

func (s *RedisSubscriber) Pull(timeout time.Duration) (string, int) {
	select {
	case m := <-s.Chan():
		return m, SUCCESS
	case <-time.After(timeout):
		return "", TIMEOUT
	}
}

func (s *RedisSubscriber) Chan() chan string {
	return s.output
}

func (s *RedisSubscriber) Stop(timeout time.Duration) {
	s.sub.Unsubscribe(s.topic)
	s.cancel()
	s.sub.Close()
}
