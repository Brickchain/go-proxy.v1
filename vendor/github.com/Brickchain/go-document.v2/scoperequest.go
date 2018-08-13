package document

import (
	"time"
)

// ScopeRequest is a request for scope data (used in OpenID Connect).
// The signer will evaluate the scopes and build the appropriate data structures
//
// If the ID field (from Base) is set, the URI it's pointing to contains the document that the client should sign.
//
// ReplyTo specifies the callback URI where to send the response. The response is a JWS where the payload is an encrypted (JWE) ScopeData.
// Scopes is a list of requested scopes
// EncryptTo specifies what recipients to encrypt the response to.
//
// Scope is a document describing a specific scope with name and optional link to where to get one of that type of fact.

const ScopeRequestType = SchemaLocation + "/scope-request.json"

type ScopeRequest struct {
	Base
	ReplyTo  []string  `json:"replyTo"`
	Scopes   []Scope   `json:"scopes"`
	KeyLevel int       `json:"keyLevel,omitempty"`
	Contract *Contract `json:"contract,omitempty"`
}

type Scope struct {
	Name     string `json:"name"`
	Link     string `json:"link,omitempty"`
	Required bool   `json:"required,omitempty"`
}

// NewScopeRequest creates a new ScopeRequest
func NewScopeRequest(keyLevel int) *ScopeRequest {
	s := &ScopeRequest{
		Base: Base{
			Type:      ScopeRequestType,
			Timestamp: time.Now().UTC(),
		},
		ReplyTo:  []string{},
		Scopes:   []Scope{},
		KeyLevel: keyLevel,
	}
	return s
}
