package document

import "time"

const ReceiptType = SchemaLocation + "/receipt.json"

type Interval struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type Receipt struct {
	Base
	Action    string            `json:"action,omitempty"`
	URI       string            `json:"viewuri,omitempty"`
	JWT       string            `json:"jwt,omitempty"`
	Intervals []Interval        `json:"intervals,omitempty"`
	Label     string            `json:"label,omitempty"`
	Params    map[string]string `json:"params,omitempty"`
}

func NewReceipt(label string) *Receipt {
	return &Receipt{
		Base: Base{
			Type:      ReceiptType,
			Timestamp: time.Now().UTC(),
		},
		Intervals: make([]Interval, 0),
		Label:     label,
	}
}
