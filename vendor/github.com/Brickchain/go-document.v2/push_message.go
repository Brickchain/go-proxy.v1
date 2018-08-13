package document

import "time"

const PushMessageType = SchemaLocation + "/push-message.json"

type PushMessage struct {
	*Base
	Title   string `json:"title"`
	Message string `json:"message"`
	URI     string `json:"uri,omitempty"`
	Data    string `json:"data,omitempty"`
}

func NewPushMessage(title, message string) *PushMessage {
	return &PushMessage{
		Base: &Base{
			Type:      PushMessageType,
			Timestamp: time.Now().UTC(),
		},
		Title:   title,
		Message: message,
	}
}
