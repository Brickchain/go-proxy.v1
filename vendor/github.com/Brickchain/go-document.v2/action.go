package document

import (
	"crypto/rand"
	"encoding/base64"
	"time"
)

const ActionType = SchemaLocation + "/action.json"

type Action struct {
	Base
	Mandates []string          `json:"mandates,omitempty"`
	Nonce    string            `json:"nonce,omitempty"`
	Params   map[string]string `json:"params,omitempty"`
	Facts    []Part            `json:"facts,omitempty"`
	Contract string            `json:"contract,omitempty"`
}

func NewAction(mandates []string) *Action {
	a := &Action{
		Base: Base{
			Type:      ActionType,
			Timestamp: time.Now().UTC(),
		},
		Mandates: mandates,
		Params:   make(map[string]string),
	}
	a.generateNonce()

	return a
}

func (a *Action) generateNonce() error {
	// From https://elithrar.github.io/article/generating-secure-random-numbers-crypto-rand/

	// GenerateRandomBytes returns securely generated random bytes.
	// It will return an error if the system's secure random
	// number generator fails to function correctly, in which
	// case the caller should not continue.
	b := make([]byte, 32)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return err
	}

	// GenerateRandomString returns a URL-safe, base64 encoded
	// securely generated random string.
	// It will return an error if the system's secure random
	// number generator fails to function correctly, in which
	// case the caller should not continue.
	a.Nonce = base64.URLEncoding.EncodeToString(b)

	return nil
}
