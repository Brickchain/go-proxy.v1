package document

import (
	"encoding/json"
	"time"

	"gopkg.in/square/go-jose.v1"
)

const RevocationRequestType = SchemaLocation + "/revocation-request.json"

type RevocationRequest struct {
	Base
	SignedDocument     *jose.JsonWebSignature `json:"jws"`                // The signature from this document is to be revoked
	RevocationChecksum *jose.JsonWebSignature `json:"revocationchecksum"` // The hashed SignedDocument wrapped in a jws signed by the same key
	Priority           int                    `json:"priority"`
}

func NewRevocationRequest(jws, checksum *jose.JsonWebSignature, prio int) *RevocationRequest {
	r := &RevocationRequest{
		Base: Base{
			Type:      RevocationRequestType,
			Timestamp: time.Now().UTC(),
		},
		SignedDocument:     jws,
		RevocationChecksum: checksum,
		Priority:           prio,
	}
	return r
}

func (r *RevocationRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Base
		SignedDocument     string `json:"jws"`                // The signature from this document is to be revoked
		RevocationChecksum string `json:"revocationchecksum"` // The hashed SignedDocument wrapped in a jws signed by the same key
		Priority           int    `json:"priority"`
	}{
		Base:               r.Base,
		SignedDocument:     r.SignedDocument.FullSerialize(),
		RevocationChecksum: r.RevocationChecksum.FullSerialize(),
		Priority:           r.Priority,
	})
}

func (r *RevocationRequest) UnmarshalJSON(data []byte) error {
	aux := &struct {
		Base
		SignedDocument     string `json:"jws"`                // The signature from this document is to be revoked
		RevocationChecksum string `json:"revocationchecksum"` // The hashed SignedDocument wrapped in a jws signed by the same key
		Priority           int    `json:"priority"`
	}{}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error
	r.Base = aux.Base
	r.SignedDocument, err = jose.ParseSigned(aux.SignedDocument)
	if err != nil {
		return err
	}
	r.RevocationChecksum, err = jose.ParseSigned(aux.RevocationChecksum)
	if err != nil {
		return err
	}
	r.Priority = aux.Priority

	return nil
}
