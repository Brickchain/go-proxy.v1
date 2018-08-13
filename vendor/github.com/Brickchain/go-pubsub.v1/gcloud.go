package pubsub

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Brickchain/go-logger.v1"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type GCloudPubSub struct {
	client  *pubsub.Client
	topics  map[string]*pubsub.Topic
	topicMu sync.Mutex
}

func NewGCloudPubSub(projectId string, serviceAccountFile string) (*GCloudPubSub, error) {
	g := GCloudPubSub{}
	ctx := context.Background()
	if os.Getenv("PUBSUB_EMULATOR_HOST") != "" {
		g.client, _ = pubsub.NewClient(ctx, projectId)
	} else {
		var clientOptions option.ClientOption
		if serviceAccountFile != "" {
			jsonKey, err := ioutil.ReadFile(serviceAccountFile)
			if err != nil {
				return nil, err
			}

			conf, err := google.JWTConfigFromJSON(jsonKey, pubsub.ScopePubSub, pubsub.ScopeCloudPlatform)
			if err != nil {
				return nil, err
			}

			ts := conf.TokenSource(ctx)
			clientOptions = option.WithTokenSource(ts)
		} else {
			// defaultClient, err := google.DefaultClient(ctx, pubsub.ScopePubSub, pubsub.ScopeCloudPlatform)
			// if err != nil {
			// 	return nil, errors.Wrap(err, "failed to create default client")
			// }
			// clientOptions = option.WithHTTPClient(defaultClient)
			clientOptions = option.WithScopes(pubsub.ScopePubSub, pubsub.ScopeCloudPlatform)
		}

		var err error
		g.client, err = pubsub.NewClient(ctx, projectId, clientOptions)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create new client")
		}
	}

	g.topics = make(map[string]*pubsub.Topic)

	return &g, nil
}

func (g *GCloudPubSub) Stop() {
	g.client.Close()
}

func (g *GCloudPubSub) Publish(topic string, data string) error {
	ctx := context.Background()

	t, err := g.ensureTopic(topic)
	if err != nil {
		return err
	}

	res := t.Publish(ctx, &pubsub.Message{
		Data: []byte(data),
	})
	if _, err := res.Get(ctx); err != nil {
		return err
	}
	// logger.Debug("Sent message:", id)

	return nil
}

func (g *GCloudPubSub) ensureTopic(topicName string) (*pubsub.Topic, error) {
	ctx := context.Background()

	// fixedName := fmt.Sprintf("t%s", hex.EncodeToString([]byte(topicName)))

	g.topicMu.Lock()
	defer g.topicMu.Unlock()
	topic, exists := g.topics[topicName]

	if !exists || topic == nil {
		topic = g.client.Topic(topicName)
		topicExists, err := topic.Exists(ctx)
		if err != nil {
			return nil, err
		}
		if !topicExists {
			// logger.Info("New topic:", topicName)
			var err error
			topic, err = g.client.CreateTopic(ctx, topicName)
			if err != nil {
				return nil, err
			}
		}
		g.topics[topicName] = topic
	}

	return topic, nil
}

func (g *GCloudPubSub) DeleteTopic(topicName string) error {

	// fixedName := fmt.Sprintf("t%s", hex.EncodeToString([]byte(topicName)))

	topic := g.client.Topic(topicName)
	topic.Delete(context.Background())

	g.topicMu.Lock()
	delete(g.topics, topicName)
	g.topicMu.Unlock()

	return nil
}

type GCloudSubscriber struct {
	g       *GCloudPubSub
	group   string
	topic   *pubsub.Topic
	output  chan string
	done    chan bool
	ready   chan bool
	running bool
}

func (g *GCloudPubSub) Subscribe(group, topic string) (Subscriber, error) {

	t, err := g.ensureTopic(topic)
	if err != nil {
		return nil, err
	}

	s := GCloudSubscriber{
		g:      g,
		group:  group,
		topic:  t,
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

func (s *GCloudSubscriber) run() {
	// logger.Info("Starting subscriber for ", s.topic.String())
	// defer logger.Info("Subscriber has stopped")
	s.running = true
	ctx := context.Background()
	sub := s.g.client.Subscription(s.group)
	if exists, _ := sub.Exists(ctx); !exists {
		var err error
		cfg := pubsub.SubscriptionConfig{
			Topic:       s.topic,
			AckDeadline: 10 * time.Second,
		}
		sub, err = s.g.client.CreateSubscription(ctx, s.group, cfg)
		if err != nil {
			logger.Error(err)
			s.ready <- false
			return
		}
	}
	if sub == nil {
		logger.Error("Subscription not created...")
		s.ready <- false
		return
	}
	go func() {
		_ = <-s.done
		// logger.Info("got done message.. stopping")
		err := sub.Delete(ctx)
		if err != nil {
			logger.Error("Failed to delete subscription: ", err)
		}
		s.running = false
	}()

	logger.Debug(sub.String())
	s.ready <- true
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// logger.Debug("Received msg: ", string(msg.Data))
		s.output <- string(msg.Data)
		msg.Ack()
	})
	if err != nil {
		logger.Error(err)
		s.ready <- false
	}
}

func (s *GCloudSubscriber) Pull(timeout time.Duration) (string, int) {
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

func (s *GCloudSubscriber) Chan() chan string {
	return s.output
}

func (s *GCloudSubscriber) Stop(timeout time.Duration) {
	s.done <- true
	// logger.Debug("Waiting for subscriber to die...")
	start := time.Now()
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
