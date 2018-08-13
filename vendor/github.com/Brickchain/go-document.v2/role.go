package document

import (
	"strings"
	"time"
)

const RoleType = SchemaLocation + "/role.json"

type Role struct {
	*Base
	Name        string `json:"name,omitempty" gorm:"index"`
	Description string `json:"description,omitempty"`
	KeyLevel    int    `json:"keyLevel,omitempty"`
}

func NewRole(name string) *Role {
	return &Role{
		Base: &Base{
			Type:      RoleType,
			Timestamp: time.Now().UTC(),
		},
		Name:        name,
		Description: strings.Title(strings.Split(name, "@")[0]),
		KeyLevel:    10,
	}
}
