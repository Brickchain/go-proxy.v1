package document

import "time"

const ContractType = SchemaLocation + "/contract.json"

type Contract struct {
	Base
	Text        string `json:"text,omitempty"`
	Attachments []Part `json:"attachments,omitempty"`
}

func NewContract() *Contract {
	return &Contract{
		Base: Base{
			Type:      ContractType,
			Timestamp: time.Now().UTC(),
		},
		Attachments: make([]Part, 0),
	}
}
