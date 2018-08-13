package document

import (
	"time"

	jose "gopkg.in/square/go-jose.v1"
)

const RealmDescriptorType = SchemaLocation + "/realm-descriptor.json"

type RealmDescriptor struct {
	Base
	Label       string           `json:"label,omitempty"`
	PublicKey   *jose.JsonWebKey `json:"publicKey,omitempty"`
	InviteURL   string           `json:"inviteURL,omitempty"`
	ServicesURL string           `json:"servicesURL,omitempty"`
	Icon        string           `json:"icon,omitempty"`
	Banner      string           `json:"banner,omitempty"`
}

func NewRealmDescriptor(id string, publicKey *jose.JsonWebKey, servicesURL string) *RealmDescriptor {
	return &RealmDescriptor{
		Base: Base{
			ID:        id,
			Type:      RealmDescriptorType,
			Timestamp: time.Now().UTC(),
		},
		PublicKey:   publicKey,
		ServicesURL: servicesURL,
	}
}
