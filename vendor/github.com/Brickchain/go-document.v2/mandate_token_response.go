package document

import (
	"time"
)

const MandateTokenResponseType = SchemaLocation + "/mandate-token-response.json"

type MandateTokenResponse struct {
	Base
	Token string `json:"token"`
}

func NewMandateTokenResponse(token string) *MandateTokenResponse {
	return &MandateTokenResponse{
		Base: Base{
			Type:      MandateTokenResponseType,
			Timestamp: time.Now().UTC(),
		},
		Token: token,
	}
}
