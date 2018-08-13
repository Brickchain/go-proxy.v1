package crypto

import "gopkg.in/square/go-jose.v1"

type Signer struct {
	sign jose.Signer
	key  *jose.JsonWebKey
}

func NewSigner(key *jose.JsonWebKey) (*Signer, error) {
	var err error

	s := Signer{
		key: key,
	}
	s.sign, err = jose.NewSigner(SIGNALG, key)
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func (s *Signer) Sign(payload []byte) (*jose.JsonWebSignature, error) {
	jws, err := s.sign.Sign(payload)
	if err != nil {
		return nil, err
	}
	// jws.Signatures[0].Header.JsonWebKey = s.key
	// jws.Signatures[0].Header.KeyID = s.key.KeyID

	return jws, nil
}

func (s *Signer) AppendSignature(sig *jose.JsonWebSignature) error {
	// get payload
	_, _, payload, err := sig.VerifyMulti(sig.Signatures[0].Header.JsonWebKey)
	if err != nil {
		return err
	}

	// sign payload
	ourSig, err := s.Sign(payload)
	if err != nil {
		return err
	}

	// append our signature to sig
	sig.Signatures = append(sig.Signatures, ourSig.Signatures...)

	return nil
}

func UnmarshalSignature(data []byte) (*jose.JsonWebSignature, error) {
	sig, err := jose.ParseSigned(string(data))
	if err != nil {
		return nil, err
	}

	return sig, nil
}
