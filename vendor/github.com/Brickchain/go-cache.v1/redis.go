package cache

import (
	"strings"
	"time"

	rediscache "gopkg.in/go-redis/cache.v5"
	redis "gopkg.in/redis.v5"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type RedisCache struct {
	ring  *redis.Ring
	codec *rediscache.Codec
}

func NewRedisCache(servers string) *RedisCache {
	serverList := make(map[string]string)
	for i, e := range strings.Split(servers, ",") {
		serverList[string(i)] = e
	}

	ring := redis.NewRing(&redis.RingOptions{
		Addrs: serverList,
	})

	r := &RedisCache{
		ring: ring,
	}

	r.codec = &rediscache.Codec{
		Redis: ring,

		Marshal: func(v interface{}) ([]byte, error) {
			return msgpack.Marshal(v)
		},
		Unmarshal: func(b []byte, v interface{}) error {
			return msgpack.Unmarshal(b, v)
		},
	}

	return r
}

func (r *RedisCache) Get(key string) (data []byte, err error) {
	err = r.codec.Get(key, &data)
	return
}

func (r *RedisCache) Set(key string, data []byte, ttl time.Duration) error {
	return r.codec.Set(&rediscache.Item{
		Key:        key,
		Object:     data,
		Expiration: ttl,
	})
}

func (r *RedisCache) GetTTL(key string) (time.Duration, error) {
	t := r.ring.TTL(key)
	return t.Result()
}

func (r *RedisCache) Extend(key string, ttl time.Duration) error {
	c := r.ring.Expire(key, ttl)
	_, err := c.Result()
	return err
}
