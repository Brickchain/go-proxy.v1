package pubsub

import (
	"fmt"
	"sync"
	"time"

	"github.com/Brickchain/go-logger.v1"

	"github.com/jinzhu/gorm"
)

type pubsubMessage struct {
	ID        int64  `gorm:"primary_key"`
	Topic     string `gorm:"index"`
	CreatedAt time.Time
	Data      string
}

type pubsubGroup struct {
	ID        string `gorm:"primary_key"`
	Topic     string `gorm:"index"`
	CreatedAt time.Time
	UpdatedAt time.Time
	Loc       int64
}

type PersistantPubSub struct {
	db    *gorm.DB
	inmem *InmemPubSub
	locks map[string]*sync.Mutex
}

func NewPersistantPubSub(db *gorm.DB) *PersistantPubSub {
	db.AutoMigrate(&pubsubMessage{}, &pubsubGroup{})

	return &PersistantPubSub{
		db:    db,
		inmem: NewInmemPubSub(),
		locks: make(map[string]*sync.Mutex),
	}
}

func (p *PersistantPubSub) Publish(topic string, data string) error {
	msg := &pubsubMessage{
		Topic: topic,
		Data:  data,
	}
	if err := p.db.Save(msg).Error; err != nil {
		return err
	}

	return p.inmem.Publish(topic, fmt.Sprintf("%v", msg.ID))
}

func (p *PersistantPubSub) Subscribe(group string, topic string) (Subscriber, error) {
	inmemsub, err := p.inmem.Subscribe(group, topic)
	if err != nil {
		return nil, err
	}

	lock, ok := p.locks[group]
	if !ok {
		lock = &sync.Mutex{}
		p.locks[group] = lock
	}

	s := &PersistantSubscriber{
		topic:    topic,
		group:    group,
		db:       p.db,
		inmem:    inmemsub,
		results:  make(chan string),
		errChan:  make(chan error),
		doneChan: make(chan struct{}),
		lock:     lock,
	}

	go s.puller()

	select {
	case err = <-s.errChan:
		return nil, err
	case <-time.After(5 * time.Second):
		return s, nil
	}
}

func (p *PersistantPubSub) DeleteTopic(topic string) error {
	return nil
}

func (p *PersistantPubSub) Stop() {
	return
}

type PersistantSubscriber struct {
	topic    string
	group    string
	db       *gorm.DB
	inmem    Subscriber
	results  chan string
	errChan  chan error
	doneChan chan struct{}
	lock     *sync.Mutex
}

func (s *PersistantSubscriber) Pull(timeout time.Duration) (string, int) {
	select {
	case msg := <-s.results:
		return msg, SUCCESS
	case err := <-s.errChan:
		logger.Error(err)
		return "", ERROR
	case <-time.After(timeout):
		return "", TIMEOUT
	}
}

func (s *PersistantSubscriber) puller() {
	defer close(s.errChan)

	s.lock.Lock()
	g := &pubsubGroup{}
	err := s.db.Where("id = ? AND topic = ?", s.group, s.topic).First(&g).Error
	if err != nil {
		g.ID = s.group
		g.Topic = s.topic
		g.Loc = 0
		s.db.Save(&g)
	}
	messages := make([]pubsubMessage, 0)
	err = s.db.Where("id > ? AND topic = ?", g.Loc, g.Topic).Find(&messages).Error
	if err != nil {
		logger.Error(err)
		s.errChan <- err
		s.lock.Unlock()
		return
	}

	for _, message := range messages {
		s.results <- message.Data
		err = s.db.Model(&g).Where("id = ? AND topic = ?", g.ID, g.Topic).Update("loc", message.ID).Error
		if err != nil {
			logger.Error(err)
			s.errChan <- err
			s.lock.Unlock()
			return
		}
	}

	s.lock.Unlock()
	for {
		select {
		case id := <-s.inmem.Chan():
			s.lock.Lock()
			message := &pubsubMessage{}
			err = s.db.Where("id = ?", id).First(&message).Error
			if err != nil {
				logger.Error(err)
				s.errChan <- err
				s.lock.Unlock()
				continue
			}

			s.results <- message.Data
			err = s.db.Model(&g).Where("id = ? AND topic = ?", g.ID, g.Topic).Update("loc", message.ID).Error
			if err != nil {
				logger.Error(err)
				s.errChan <- err
				s.lock.Unlock()
				continue
			}
			s.lock.Unlock()
		case <-s.doneChan:
			s.inmem.Stop(1 * time.Second)
			return
		}
	}
}

func (s *PersistantSubscriber) Stop(timeout time.Duration) {
	close(s.doneChan)
	select {
	case <-s.errChan:
		return
	case <-time.After(timeout):
		return
	}
}

func (s *PersistantSubscriber) Chan() chan string {
	return s.results
}
