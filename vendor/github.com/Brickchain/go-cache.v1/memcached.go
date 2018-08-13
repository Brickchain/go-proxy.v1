package cache

import (
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

type Memcached struct {
	client *memcache.Client
}

func NewMemcached(server ...string) *Memcached {
	m := &Memcached{
		client: memcache.New(server...),
	}

	return m
}

func (m *Memcached) Get(key string) ([]byte, error) {
	item, err := m.client.Get(key)
	if err != nil {
		return nil, err
	}

	return item.Value, nil
}

func (m *Memcached) Set(key string, data []byte, ttl time.Duration) error {
	return m.client.Set(&memcache.Item{
		Key:        key,
		Value:      data,
		Expiration: int32(ttl.Seconds()),
	})
}

func (m *Memcached) GetTTL(key string) (time.Duration, error) {
	return 0, fmt.Errorf("GetTTL not available on Memcached")
}

func (m *Memcached) Extend(key string, ttl time.Duration) error {
	return m.client.Touch(key, int32(ttl.Seconds()))
}
