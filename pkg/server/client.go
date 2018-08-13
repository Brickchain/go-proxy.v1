package server

import (
	crypto "github.com/Brickchain/go-crypto.v2"
	jose "gopkg.in/square/go-jose.v1"
)

type Client struct {
	ID  string
	Key *jose.JsonWebKey
}

func NewClient(key *jose.JsonWebKey) *Client {
	return &Client{
		ID:  crypto.Thumbprint(key),
		Key: key,
	}
}
