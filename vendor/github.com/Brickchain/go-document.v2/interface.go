package document

import (
	"encoding/json"
	"strings"
	"time"
)

// SchemaLocation is where we host our document schema
const SchemaLocation = "https://schema.brickchain.com/v2"

// Document describes the base types on a document
type Document interface {
	GetType() string
	GetTimestamp() time.Time
	GetCertificate() string
	GetRaw() []byte
	SetRaw([]byte)
}

func Marshal(doc Document) []byte {
	docBytes, _ := json.Marshal(doc)
	return docBytes
}

func Unmarshal(data []byte) (Document, error) {
	base := &Base{}
	if err := json.Unmarshal(data, &base); err != nil {
		return base, err
	}

	base.SetRaw(data)

	var typ Document
	switch strings.Split(base.Type, "#")[0] {
	case SchemaLocation + "/base.json":
		return base, nil
	case SchemaLocation + "/action.json":
		typ = &Action{}
	case SchemaLocation + "/action-descriptor.json":
		typ = &ActionDescriptor{}
	case SchemaLocation + "/certificate.json":
		typ = &Certificate{}
	case SchemaLocation + "/controller-binding.json":
		typ = &ControllerBinding{}
	case SchemaLocation + "/controller-descriptor.json":
		typ = &ControllerDescriptor{}
	case SchemaLocation + "/fact.json":
		typ = &Fact{}
	case SchemaLocation + "/mandate.json":
		typ = &Mandate{}
	case SchemaLocation + "/mandate-token.json":
		typ = &MandateToken{}
	case SchemaLocation + "/message.json":
		typ = &Message{}
	case SchemaLocation + "/multipart.json":
		typ = &Multipart{}
	case SchemaLocation + "/realm-descriptor.json":
		typ = &RealmDescriptor{}
	case SchemaLocation + "/receipt.json":
		typ = &Receipt{}
	case SchemaLocation + "/revocation-checksum.json":
		typ = &RevocationChecksum{}
	case SchemaLocation + "/revocation-request.json":
		typ = &RevocationRequest{}
	case SchemaLocation + "/revocation.json":
		typ = &Revocation{}
	case SchemaLocation + "/scope-request.json":
		typ = &ScopeRequest{}
	case SchemaLocation + "/signature-request.json":
		typ = &SignatureRequest{}
	default:
		return base, nil
	}

	if err := json.Unmarshal(data, &typ); err != nil {
		return base, err
	}
	typ.SetRaw(data)

	return typ, nil
}

func GetType(data []byte) (string, error) {
	b, err := Unmarshal(data)
	return b.GetType(), err
}
