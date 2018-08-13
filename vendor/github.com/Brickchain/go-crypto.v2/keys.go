package crypto

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base32"
	"encoding/pem"
	"strings"

	hash "crypto"

	"github.com/pkg/errors"
	jose "gopkg.in/square/go-jose.v1"
)

func NewKey() (*jose.JsonWebKey, error) {
	key, err := ecdsa.GenerateKey(CURVE, rand.Reader)
	if err != nil {
		return nil, err
	}

	jwk := jose.JsonWebKey{
		Key:       key,
		Algorithm: string(SIGNALG),
	}

	return &jwk, nil
}

func NewPublicKey(key *jose.JsonWebKey) (*jose.JsonWebKey, error) {
	var publicKey interface{}
	switch key.Key.(type) {
	case *ecdsa.PrivateKey:
		publicKey = &key.Key.(*ecdsa.PrivateKey).PublicKey
	case *ecdsa.PublicKey:
		publicKey = key.Key.(*ecdsa.PublicKey)
	case *rsa.PrivateKey:
		publicKey = &key.Key.(*rsa.PrivateKey).PublicKey
	case *rsa.PublicKey:
		publicKey = key.Key.(*rsa.PublicKey)
	}
	jwk := jose.JsonWebKey{
		Key:   publicKey,
		KeyID: key.KeyID,
	}

	if key.Algorithm != "" {
		jwk.Algorithm = key.Algorithm
	}

	return &jwk, nil
}

func NewKeySet(key *jose.JsonWebKey) (*jose.JsonWebKeySet, error) {
	pk, err := NewPublicKey(key)
	if err != nil {
		return nil, err
	}
	publicKeys := &jose.JsonWebKeySet{
		Keys: []jose.JsonWebKey{
			jose.JsonWebKey{
				Key:       pk.Key,
				KeyID:     pk.KeyID,
				Algorithm: pk.Algorithm,
			},
		},
	}

	return publicKeys, nil
}

func NewSymmetricKey(alg jose.KeyAlgorithm) []byte {
	var size int
	switch alg {
	case jose.A256KW:
		size = 32
	case jose.A128KW:
		size = 16
	}

	b := make([]byte, size)
	rand.Read(b)

	return b
}

func UnmarshalKey(data []byte) (*jose.JsonWebKey, error) {
	jwk := jose.JsonWebKey{}
	err := jwk.UnmarshalJSON(data)
	if err != nil {
		return nil, err
	}

	if jwk.Algorithm == "" {
		jwk.Algorithm = "ES256"
	}

	return &jwk, nil
}

func MarshalKey(key *jose.JsonWebKey) []byte {
	j, _ := key.MarshalJSON()
	return j
}

func SignKey(signerKey *jose.JsonWebKey, key *jose.JsonWebKey) (*jose.JsonWebSignature, error) {
	signer, err := NewSigner(signerKey)
	if err != nil {
		return nil, err
	}

	keyBytes, err := key.MarshalJSON()
	if err != nil {
		return nil, err
	}

	sig, err := signer.Sign(keyBytes)
	if err != nil {
		return nil, err
	}

	return sig, nil
}

func Thumbprint(key *jose.JsonWebKey) string {
	keyTPbytes, _ := key.Thumbprint(hash.SHA256)
	// encoder := *base32.StdEncoding
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(keyTPbytes))
	// return hex.EncodeToString(keyTPbytes)
}

func MarshalToPEM(key *jose.JsonWebKey) ([]byte, error) {

	var keyBlock pem.Block

	switch x := key.Key.(type) {
	case *ecdsa.PrivateKey:
		keyDer, err := x509.MarshalECPrivateKey(x)
		if err != nil {
			return nil, err
		}

		keyBlock = pem.Block{
			Type:  "EC PRIVATE KEY",
			Bytes: keyDer,
		}
	case *rsa.PrivateKey:
		keyDer := x509.MarshalPKCS1PrivateKey(x)

		keyBlock = pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: keyDer,
		}
	default:
		return nil, errors.New("Unknown key type")
	}

	return pem.EncodeToMemory(&keyBlock), nil
}

func UnmarshalPEM(b []byte) (*jose.JsonWebKey, error) {

	key, err := jose.LoadPrivateKey(b)
	if err != nil {
		return nil, err
	}

	jwk := &jose.JsonWebKey{
		Key:       key,
		Algorithm: string(SIGNALG),
	}

	return jwk, nil
}
