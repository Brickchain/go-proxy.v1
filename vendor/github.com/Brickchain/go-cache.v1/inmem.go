package cache

import (
	"fmt"
	"sync"
	"time"
)

type Value struct {
	Data   []byte    `json:"data"`
	Expiry time.Time `json:"expiry"`
}

type Inmem struct {
	data map[string]*Value
	mu   sync.Mutex
}

func NewInmem() *Inmem {
	i := &Inmem{
		data: make(map[string]*Value),
		mu:   sync.Mutex{},
	}

	go i.reaper()

	return i
}

func (i *Inmem) reaper() {
	for {
		i.mu.Lock()
		for key, val := range i.data {
			if val.Expiry.Before(time.Now()) {
				delete(i.data, key)
			}
		}
		i.mu.Unlock()
		time.Sleep(time.Second * 10)
	}
}

func (i *Inmem) Get(key string) ([]byte, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	val, ok := i.data[key]
	if !ok {
		return nil, fmt.Errorf("Key not found")
	}

	if val.Expiry.Before(time.Now()) {
		return nil, fmt.Errorf("Key not found")
	}

	return val.Data, nil
}

func (i *Inmem) Set(key string, data []byte, ttl time.Duration) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	val := &Value{
		Data:   data,
		Expiry: time.Now().Add(ttl),
	}

	i.data[key] = val

	return nil
}

func (i *Inmem) GetTTL(key string) (time.Duration, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	val, ok := i.data[key]
	if !ok {
		return time.Duration(0), fmt.Errorf("Key not found")
	}

	return val.Expiry.Sub(time.Now()), nil
}

func (i *Inmem) Extend(key string, ttl time.Duration) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	val, ok := i.data[key]
	if !ok {
		return fmt.Errorf("Key not found")
	}

	val.Expiry = time.Now().Add(ttl)

	i.data[key] = val

	return nil
}
