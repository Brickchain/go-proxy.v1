package document

import (
	"time"
)

const MandateTokenRequestType = SchemaLocation + "/mandate-token-request.json"

type MandateTokenRequest struct {
	Base
	Roles    []string  `json:"roles"`
	Contract *Contract `json:"contract,omitempty"`
	URI      string    `json:"uri,omitempty"`
	TTL      int       `json:"ttl,omitempty"`
	ReplyTo  []string  `json:"replyTo"`
}

func NewMandateTokenRequest(roles []string, uri string, ttl int) *MandateTokenRequest {
	return &MandateTokenRequest{
		Base: Base{
			Type:      MandateTokenRequestType,
			Timestamp: time.Now().UTC(),
		},
		Roles: roles,
		URI:   uri,
		TTL:   ttl,
	}
}
