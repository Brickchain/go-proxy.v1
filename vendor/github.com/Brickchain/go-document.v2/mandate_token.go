package document

import (
	"time"
)

const MandateTokenType = SchemaLocation + "/mandate-token.json"

type MandateToken struct {
	Base
	Mandates []string `json:"mandates,omitempty"`
	URI      string   `json:"uri,omitempty"`
	TTL      int      `json:"ttl,omitempty"`
}

func NewMandateToken(mandates []string, uri string, ttl int) *MandateToken {
	return &MandateToken{
		Base: Base{
			Type:      MandateTokenType,
			Timestamp: time.Now().UTC(),
		},
		Mandates: mandates,
		URI:      uri,
		TTL:      ttl,
	}
}
