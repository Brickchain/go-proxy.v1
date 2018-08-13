package crypto

import (
	"gopkg.in/square/go-jose.v1"
)

type Encrypter struct {
	menc jose.MultiEncrypter
}

type SymmetricEncrypter struct {
	enc jose.Encrypter
}

func NewEncrypter() (*Encrypter, error) {
	enc, err := jose.NewMultiEncrypter(CONTENTALG)
	if err != nil {
		return nil, err
	}

	e := Encrypter{
		menc: enc,
	}

	return &e, nil
}

func NewSymmetricEncrypter(key []byte) (*SymmetricEncrypter, error) {
	enc, err := jose.NewEncrypter(SYMKEYALG, CONTENTALG, key)
	if err != nil {
		return nil, err
	}

	s := SymmetricEncrypter{
		enc: enc,
	}

	return &s, nil
}

func (e *Encrypter) AddRecipient(pk *jose.JsonWebKey) error {
	err := e.menc.AddRecipient(KEYALG, pk)
	if err != nil {
		return err
	}

	return nil
}

func (e  *Encrypter) Encrypt(data []byte) (*jose.JsonWebEncryption, error) {
	msg, err := e.menc.Encrypt(data)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (e  *SymmetricEncrypter) Encrypt(data []byte) (*jose.JsonWebEncryption, error) {
	msg, err := e.enc.Encrypt(data)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func UnmarshalJWE(data string) (*jose.JsonWebEncryption, error) {
	jwe, err := jose.ParseEncrypted(data)
	if err != nil {
		return nil, err
	}

	return jwe, nil
}