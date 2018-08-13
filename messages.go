package proxy

import (
	"time"

	document "github.com/Brickchain/go-document.v2"
	uuid "github.com/satori/go.uuid"
)

const SchemaBase = "https://proxy.brickchain.com/v1"

type HttpRequest struct {
	document.Base
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Method  string            `json:"method"`
	Body    string            `json:"body"`
}

func NewHttpRequest(url string) *HttpRequest {
	return &HttpRequest{
		Base: document.Base{
			ID:        uuid.Must(uuid.NewV4()).String(),
			Type:      SchemaBase + "/http-request.json",
			Timestamp: time.Now().UTC(),
		},
		URL: url,
	}
}

type HttpResponse struct {
	document.Base
	Headers     map[string]string `json:"headers"`
	ContentType string            `json:"contentType"`
	Status      int               `json:"status"`
	Body        string            `json:"body"`
}

func NewHttpResponse(id string, status int) *HttpResponse {
	return &HttpResponse{
		Base: document.Base{
			ID:        id,
			Type:      SchemaBase + "/http-response.json",
			Timestamp: time.Now().UTC(),
		},
		Status: status,
	}
}

type RegistrationRequest struct {
	document.Base
	MandateToken string `json:"mandateToken"`
}

func NewRegistrationRequest(mandateToken string) *RegistrationRequest {
	return &RegistrationRequest{
		Base: document.Base{
			ID:        uuid.Must(uuid.NewV4()).String(),
			Type:      SchemaBase + "/registration-request.json",
			Timestamp: time.Now().UTC(),
		},
		MandateToken: mandateToken,
	}
}

type RegistrationResponse struct {
	document.Base
	KeyID    string `json:"keyID"`
	Hostname string `json:"hostname,omitempty"`
}

func NewRegistrationResponse(id string, keyID string) *RegistrationResponse {
	return &RegistrationResponse{
		Base: document.Base{
			ID:        id,
			Type:      SchemaBase + "/registration-response.json",
			Timestamp: time.Now().UTC(),
		},
		KeyID: keyID,
	}
}

type Ping struct {
	document.Base
}

func NewPing() *Ping {
	return &Ping{
		Base: document.Base{
			ID:        uuid.Must(uuid.NewV4()).String(),
			Type:      SchemaBase + "/ping.json",
			Timestamp: time.Now().UTC(),
		},
	}
}
