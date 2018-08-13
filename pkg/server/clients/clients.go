package clients

import (
	"encoding/json"
	"fmt"
	"time"

	cache "github.com/Brickchain/go-cache.v1"
	logger "github.com/Brickchain/go-logger.v1"
	"github.com/Brickchain/go-proxy.v1/pkg/server"
	"github.com/pkg/errors"
)

type ClientService struct {
	cache cache.Cache
}

func NewClientService(cache cache.Cache) *ClientService {
	return &ClientService{
		cache: cache,
	}
}

func (c *ClientService) Get(id string) (*server.Client, error) {
	logger.Debug("Get client: ", id)
	b, err := c.cache.Get(fmt.Sprintf("/proxy/clients/%s", id))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client from cache")
	}

	client := &server.Client{}
	if err := json.Unmarshal(b, &client); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal client")
	}

	return client, nil
}

func (c *ClientService) Set(client *server.Client) error {
	b, err := json.Marshal(client)
	if err != nil {
		return errors.Wrap(err, "failed to marshal client")
	}

	if err := c.cache.Set(fmt.Sprintf("/proxy/clients/%s", client.ID), b, time.Minute*1); err != nil {
		return errors.Wrap(err, "failed to write client to cache")
	}

	return nil
}

func (c *ClientService) RenewTTL(id string) error {
	return c.cache.Extend(fmt.Sprintf("/proxy/clients/%s", id), time.Minute*1)
}
