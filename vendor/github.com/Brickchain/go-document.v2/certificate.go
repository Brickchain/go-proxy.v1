package document

import (
	"time"

	"strings"

	jose "gopkg.in/square/go-jose.v1"
)

const CertificateType = SchemaLocation + "/certificate.json"

type Certificate struct {
	Base
	TTL           int              `json:"ttl,omitempty"`
	Issuer        *jose.JsonWebKey `json:"issuer,omitempty"`
	Subject       *jose.JsonWebKey `json:"subject,omitempty"`
	DocumentTypes []string         `json:"documentTypes,omitempty"`
	KeyLevel      int              `json:"keyLevel,omitempty"`
}

func NewCertificate(issuer, subject *jose.JsonWebKey, keyLevel int, ttl int) *Certificate {
	return &Certificate{
		Base: Base{
			Type:      CertificateType,
			Timestamp: time.Now().UTC(),
		},
		Issuer:        issuer,
		Subject:       subject,
		DocumentTypes: []string{"*"},
		TTL:           ttl, // TODO: Should this be Until with time.Time instead?
		KeyLevel:      keyLevel,
	}
}

func (c *Certificate) HasExpired() bool {
	return time.Now().UTC().After(c.Timestamp.Add(time.Second * time.Duration(c.TTL)))
}

func (c *Certificate) AllowedType(doc Document) bool {
	docParts := strings.Split(doc.GetType(), "#")
	for _, allowedType := range c.DocumentTypes {
		if strings.Contains(allowedType, "#") {
			allowedParts := strings.Split(allowedType, "#")
			if len(allowedParts) < 2 {
				allowedParts = append(allowedParts, "*")
			}
			if allowedParts[0] == "*" || allowedParts[0] == docParts[0] {
				if allowedParts[1] == "*" {
					return true
				} else if len(docParts) > 1 && allowedParts[1] == docParts[1] {
					return true
				}
			}
		} else {
			if doc.GetType() == allowedType || allowedType == "*" {
				return true
			}
		}
	}
	return false
}
