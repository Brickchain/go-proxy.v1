package document

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	jose "gopkg.in/square/go-jose.v1"
)

const FactType = SchemaLocation + "/fact.json"

type Fact struct {
	Base
	Label      string                 `json:"label,omitempty"`
	Data       map[string]interface{} `json:"data"`
	Signatures []string               `json:"signatures"` // List of compact JWSes of FactSignature objects
}

type FactSignature struct {
	Certificate string            `json:"@certificate"`
	Timestamp   time.Time         `json:"@timestamp"`
	Expires     time.Time         `json:"expires,omitempty"`
	Subject     *jose.JsonWebKey  `json:"subject"`
	Hash        string            `json:"hash"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func NewFact(subType string) *Fact {
	return &Fact{
		Base: Base{
			Type:      fmt.Sprintf("%s#%s", FactType, subType),
			Timestamp: time.Now().UTC(),
		},
		Data:       make(map[string]interface{}),
		Signatures: make([]string, 0),
	}
}

func (f *Fact) HashData() (string, error) {
	h, err := f.serialize(f.Data)
	if err != nil {
		return "", err
	}

	hasher := sha256.New()
	hasher.Write([]byte(h))
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (f *Fact) serialize(data map[string]interface{}) (string, error) {
	keys := make([]string, 0)
	for key := range data {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	out := make([]string, 0)
	for _, key := range keys {
		switch x := data[key].(type) {
		case string:
			out = append(out, fmt.Sprintf("%s:%s", key, x))
		case []interface{}:
			o := make([]string, 0)
			for _, v := range x {
				switch s := v.(type) {
				case string:
					o = append(o, s)
				case map[string]interface{}:
					r, err := f.serialize(s)
					if err != nil {
						return "", err
					}
					o = append(o, r)
				default:
					return "", fmt.Errorf("Values can only be string, []string or map[string]interface{}, not %s", reflect.TypeOf(x))
				}
			}
			out = append(out, fmt.Sprintf("%s:[%s]", key, strings.Join(o, "|")))
		case map[string]interface{}:
			val, err := f.serialize(x)
			if err != nil {
				return "", err
			}
			out = append(out, fmt.Sprintf("%s:%s", key, val))
		default:
			return "", fmt.Errorf("Values can only be string, []string or map[string]interface{}, not %s", reflect.TypeOf(x))
		}
	}

	return fmt.Sprintf("{%s}", strings.Join(out, "|")), nil
}
