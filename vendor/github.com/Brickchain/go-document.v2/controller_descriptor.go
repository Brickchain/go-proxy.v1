package document

import (
	"time"

	jose "gopkg.in/square/go-jose.v1"
)

const ControllerDescriptorType = SchemaLocation + "/controller-descriptor.json"

type KeyPurpose struct {
	DocumentType string `json:"documentType"`
	Required     bool   `json:"required,omitempty"`
	Description  string `json:"description,omitempty"`
}

type ControllerDescriptor struct {
	Base
	Label              string           `json:"label"`
	ActionsURI         string           `json:"actionsURI"`
	AdminUI            string           `json:"adminUI,omitempty"`
	BindURI            string           `json:"bindURI,omitempty"`
	Key                *jose.JsonWebKey `json:"key,omitempty"`
	KeyPurposes        []KeyPurpose     `json:"keyPurposes,omitempty"`
	Status             string           `json:"status"`
	AddBindingEndpoint string           `json:"addBindingEndpoint,omitempty"`
	Icon               string           `json:"icon,omitempty"`
}

func NewControllerDescriptor(label string) *ControllerDescriptor {
	return &ControllerDescriptor{
		Base: Base{
			Type:      ControllerDescriptorType,
			Timestamp: time.Now().UTC(),
		},
		Label:       label,
		KeyPurposes: make([]KeyPurpose, 0),
	}
}
