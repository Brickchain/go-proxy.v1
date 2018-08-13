package document

import (
	"time"
)

const ControllerBindingType = SchemaLocation + "/controller-binding.json"

type ControllerBinding struct {
	Base
	RealmDescriptor       *RealmDescriptor `json:"realmDescriptor"`
	AdminRoles            []string         `json:"adminRoles,omitempty"`
	ControllerCertificate string           `json:"controllerCertificate,omitempty"`
	Mandates              []string         `json:"mandates,omitempty"`
}

func NewControllerBinding(realmDescriptor *RealmDescriptor) *ControllerBinding {
	return &ControllerBinding{
		Base: Base{
			Type:      ControllerBindingType,
			Timestamp: time.Now().UTC(),
		},
		RealmDescriptor: realmDescriptor,
	}
}
