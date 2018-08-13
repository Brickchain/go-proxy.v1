package document

import "time"

const MessageType = SchemaLocation + "/message.json"

type Message struct {
	Base
	Title   string `json:"title,omitempty"`
	Message string `json:"message,omitempty"`
}

func NewMessage(title, message string) *Message {
	return &Message{
		Base: Base{
			Type:      MessageType,
			Timestamp: time.Now().UTC(),
		},
		Title:   title,
		Message: message,
	}
}
