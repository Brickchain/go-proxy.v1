package document

import (
	"time"

	jose "gopkg.in/square/go-jose.v1"
)

const LoginRequestType = SchemaLocation + "/login-request.json"

type LoginRequest struct {
	Base
	Roles    []string         `json:"roles"`
	Contract *Contract        `json:"contract,omitempty"`
	Key      *jose.JsonWebKey `json:"key,omitempty"`
	TTL      int              `json:"ttl,omitempty"`
	ReplyTo  []string         `json:"replyTo"`
}

func NewLoginRequest(roles []string, key *jose.JsonWebKey, ttl int) *LoginRequest {
	return &LoginRequest{
		Base: Base{
			Type:      LoginRequestType,
			Timestamp: time.Now().UTC(),
		},
		Roles: roles,
		Key:   key,
		TTL:   ttl,
	}
}
