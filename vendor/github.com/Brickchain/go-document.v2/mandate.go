package document

import (
	"strings"
	"time"

	jose "gopkg.in/square/go-jose.v1"
)

const MandateType = SchemaLocation + "/mandate.json"

const (
	MandateActive  = iota
	MandateRevoked = iota
)

type Mandate struct {
	Base
	Role       string            `json:"role,omitempty"`
	RoleName   string            `json:"roleName,omitempty"`
	ValidFrom  *time.Time        `json:"validFrom,omitempty"`
	ValidUntil *time.Time        `json:"validUntil,omitempty"`
	Recipient  *jose.JsonWebKey  `json:"recipient,omitempty"`
	Sender     string            `json:"sender,omitempty"`
	Params     map[string]string `json:"params,omitempty"`
}

// use this so that we can change it in the future.
func RealmRoleFormat(roleName string, realmName string) string {
	return roleName + "@" + realmName
}

func RealmRoleParse(realmRole string) (role string, realm string) {
	parts := strings.Split(realmRole, "@")
	role = parts[0]
	if len(parts) > 1 {
		realm = parts[1]
	} else {
		realm = ""
	}
	return role, realm
}

func NewMandate(role string) *Mandate {
	now := time.Now().UTC()
	return &Mandate{
		Base: Base{
			Type:      MandateType,
			Timestamp: time.Now().UTC(),
		},
		Role:      role,
		ValidFrom: &now,
	}
}
