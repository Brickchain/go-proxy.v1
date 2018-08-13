package document

import (
	"time"
)

const RevocationChecksumType = SchemaLocation + "/revocation-checksum.json"

type RevocationChecksum struct {
	Base
	Multihash string `json:"multihash"` // The signature from this document is to be revoked
}

func NewRevocationChecksum(multihash string) *RevocationChecksum {
	r := &RevocationChecksum{
		Base: Base{
			Type:      RevocationChecksumType,
			Timestamp: time.Now().UTC(),
		},
		Multihash: multihash,
	}
	return r
}
