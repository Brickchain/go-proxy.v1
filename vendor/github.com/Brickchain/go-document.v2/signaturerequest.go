package document

import (
	"time"
)

const SignatureRequestType = SchemaLocation + "/signature-request.json"

type SignatureRequest struct {
	Base
	ReplyTo  []string  `json:"replyTo"`
	Contract *Contract `json:"contract"`
	KeyLevel int       `json:"keyLevel,omitempty"`
}

func NewSignatureRequest(keyLevel int) *SignatureRequest {
	s := &SignatureRequest{
		Base: Base{
			Type:      SignatureRequestType,
			Timestamp: time.Now(),
		},
		ReplyTo:  []string{},
		KeyLevel: keyLevel,
	}
	return s
}

const SignatureResponseType = "signature-response"

type SignatureResponse struct {
	Base
	Document string `json:"document"`
}

func NewSignatureResponse() *SignatureResponse {
	s := &SignatureResponse{
		Base: Base{
			Type:      SignatureResponseType,
			Timestamp: time.Now().UTC(),
		},
	}
	return s
}
