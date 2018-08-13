package document

import (
	"time"
)

const MultipartType = SchemaLocation + "/multipart.json"

type Multipart struct {
	*Base
	Parts []Part `json:"parts"`
}

type Part struct {
	Encoding string `json:"encoding,omitempty"`
	Name     string `json:"name,omitempty"`
	Document string `json:"document,omitempty"`
}

func NewMultipart() *Multipart {
	m := &Multipart{
		Base: &Base{
			Type:      MultipartType,
			Timestamp: time.Now().UTC(),
		},
		Parts: make([]Part, 0),
	}

	return m
}

func (m *Multipart) Append(part Part) {
	m.Parts = append(m.Parts, part)
}

func (m *Multipart) AppendDoc(doc Document) {
	p := Part{
		Encoding: "application/json",
		Document: string(Marshal(doc)),
	}
	m.Append(p)
}
