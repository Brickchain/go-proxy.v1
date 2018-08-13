package crypto

import (
	"crypto/elliptic"
	"gopkg.in/square/go-jose.v1"
)

var (
	CURVE = elliptic.P256()
	CONTENTALG = jose.A256GCM
	KEYALG = jose.ECDH_ES_A256KW
	SIGNALG = jose.ES256
	SYMKEYALG = jose.A256KW
)
