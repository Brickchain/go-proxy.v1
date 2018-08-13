package pubsub

import (
	"fmt"
	"time"

	"github.com/Brickchain/go-logger.v1"
	"gopkg.in/redis.v5"
)

type RedisPubSub struct {
	client *redis.Client
	addr   string
}

func NewRedisPubSub(addr string) (*RedisPubSub, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
		DB:       0,  // use default DB
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
	done    chan bool
	ready   chan bool
	running bool
}

func (r *RedisPubSub) Subscribe(group, topic string) (Subscriber, error) {

	client := redis.NewClient(&redis.Options{
		Addr:     r.addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	s := RedisSubscriber{
		client: client,
		topic:  topic,
		output: make(chan string),
		done:   make(chan bool),
		ready:  make(chan bool),
	}
	go s.run()

	// wait for subscriber to tell us it's ready
	ok := <-s.ready
	if !ok {
		return nil, fmt.Errorf("Something went wrong while setting up the subscriber")
	}

	return &s, nil
}

func (s *RedisSubscriber) run() {
	// logger.Info("Starting subscriber for ", s.topic)
	// defer logger.Info("Subscriber has stopped")

	s.running = true
	defer func() {
		s.running = false
	}()

	var err error
	s.sub, err = s.client.Subscribe(s.topic)
	if err != nil {
		logger.Error("Subscription not created...")
		s.ready <- false
		return
	}

	s.ready <- true
	for {
		msg, err := s.sub.ReceiveMessage()
		if err != nil {
			logger.Error(err)
			s.ready <- false
			return
		}
		s.output <- msg.Payload

		select {
		case _ = <-s.done:
			// logger.Debug("Received stop signal")
			return
		}
	}
}

func (s *RedisSubscriber) Pull(timeout time.Duration) (string, int) {
	var msg string
	select {
	case msg = <-s.output:
	case <-time.After(time.Second * timeout):
	}
	if msg == "" {
		return "", TIMEOUT
	}

	return msg, SUCCESS
}

func (s *RedisSubscriber) Chan() chan string {
	return s.output
}

func (s *RedisSubscriber) Stop(timeout time.Duration) {
	s.sub.Close()
	// logger.Debug("Waiting for subscriber to die...")
	start := time.Now()
	s.done <- true
	for {
		if start.After(start.Add(timeout)) {
			break
		}
		if !s.running {
			break
		}
		time.Sleep(time.Second * 1)
	}
	// logger.Debug("Subscriber dead!")
}
