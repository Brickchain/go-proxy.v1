package cache

import "time"

type Cache interface {
	Get(key string) ([]byte, error)
	Set(key string, data []byte, ttl time.Duration) error
	GetTTL(key string) (time.Duration, error)
	Extend(key string, ttl time.Duration) error
}
