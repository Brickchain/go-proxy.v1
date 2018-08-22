package server

import (
	hash "crypto"
	"encoding/base32"
	"strings"

	crypto "github.com/Brickchain/go-crypto.v2"
	jose "gopkg.in/square/go-jose.v1"
)

type Client struct {
	ID  string
	Key *jose.JsonWebKey
}

func NewClient(key *jose.JsonWebKey, session string) *Client {
	id := crypto.Thumbprint(key)
	if session != "" {
		h := hash.SHA256.New()
		h.Write([]byte(id))
		h.Write([]byte(session))

		id = strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(h.Sum(nil)))
	}
	return &Client{
		ID:  id,
		Key: key,
	}
}
