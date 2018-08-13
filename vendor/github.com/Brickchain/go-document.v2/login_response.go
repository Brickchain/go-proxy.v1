package document

import (
	"time"
)

const LoginResponseType = SchemaLocation + "/login-response.json"

type LoginResponse struct {
	Base
	Chain    string   `json:"chain"`
	Mandates []string `json:"mandates"`
}

func NewLoginResponse() *LoginResponse {
	return &LoginResponse{
		Base: Base{
			Type:      LoginResponseType,
			Timestamp: time.Now().UTC(),
		},
	}
}
