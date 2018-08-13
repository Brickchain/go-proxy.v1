package document

import "time"

const ActionDescriptorType = SchemaLocation + "/action-descriptor.json"

type ActionDescriptor struct {
	Base
	Label      string            `json:"label"`
	Roles      []string          `json:"roles"`
	UIURI      string            `json:"uiURI,omitempty"`
	ActionURI  string            `json:"actionURI,omitempty"`
	RefreshURI string            `json:"refreshURI,omitempty"`
	Params     map[string]string `json:"params,omitempty"`
	Scopes     []Scope           `json:"scopes,omitempty"`
	Icon       string            `json:"icon,omitempty"`
	KeyLevel   int               `json:"keyLevel,omitempty"`
	Internal   bool              `json:"internal,omitempty"`
	Contract   *Contract         `json:"contract,omitempty"`
	Interfaces []string          `json:"interfaces,omitempty"`
}

func NewActionDescriptor(label string, roles []string, keyLevel int, actionURI string) *ActionDescriptor {
	return &ActionDescriptor{
		Base: Base{
			Type:      ActionDescriptorType,
			Timestamp: time.Now().UTC(),
		},
		Label:     label,
		Roles:     roles,
		ActionURI: actionURI,
		KeyLevel:  keyLevel,
	}
}
