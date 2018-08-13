package crypto

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	jose "gopkg.in/square/go-jose.v1"

	"fmt"

	"github.com/Brickchain/go-document.v2"
)

func VerifyDocumentInJWS(docString string, keyLevel int) (document.Document, []*jose.JsonWebKey, *jose.JsonWebKey, error) {
	jws, err := UnmarshalSignature([]byte(docString))
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to unmarshal JWS")
	}

	if len(jws.Signatures) < 1 {
		return nil, nil, nil, errors.New("No signers of JWS")
	}

	signer := jws.Signatures[0].Header.JsonWebKey

	_, _, payload, err := jws.VerifyMulti(signer)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to verify signature")
	}

	doc, err := document.Unmarshal(payload)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to unmarshal document")
	}

	if doc.GetCertificate() == "" {
		return doc, []*jose.JsonWebKey{signer}, signer, nil
	}

	ok, signers, subject, err := VerifyDocumentWithCertificateChain(doc, keyLevel)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to validate certificate chain")
	}

	if !ok {
		return nil, nil, nil, errors.New("Validation of certificate chain failed")
	}

	signTP := Thumbprint(signer)
	subTP := Thumbprint(subject)
	if signTP != subTP {
		return nil, nil, nil, errors.New("Signer of document is not the subject of the certificate")
	}

	return doc, signers, subject, nil
}

func VerifyDocumentWithTypeInJWS(docString string, keyLevel int, doc document.Document) ([]*jose.JsonWebKey, *jose.JsonWebKey, error) {
	jws, err := UnmarshalSignature([]byte(docString))
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to unmarshal JWS")
	}

	if len(jws.Signatures) < 1 {
		return nil, nil, errors.New("No signers of JWS")
	}

	signer := jws.Signatures[0].Header.JsonWebKey

	_, _, payload, err := jws.VerifyMulti(signer)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to verify signature")
	}

	err = json.Unmarshal(payload, &doc)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to unmarshal document")
	}

	if doc.GetCertificate() == "" {
		return []*jose.JsonWebKey{signer}, signer, nil
	}

	ok, signers, subject, err := VerifyDocumentWithCertificateChain(doc, keyLevel)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to validate certificate chain")
	}

	if !ok {
		return nil, nil, errors.New("Validation of certificate chain failed")
	}

	signTP := Thumbprint(signer)
	subTP := Thumbprint(subject)
	if signTP != subTP {
		return nil, nil, errors.New("Signer of document is not the subject of the certificate")
	}

	return signers, subject, nil
}

func VerifyDocumentWithCertificateChain(doc document.Document, keyLevel int) (bool, []*jose.JsonWebKey, *jose.JsonWebKey, error) {
	if doc.GetCertificate() == "" {
		return false, nil, nil, errors.New("No certificateChain in document")
	}

	var subject *jose.JsonWebKey
	signers := make([]*jose.JsonWebKey, 0)
	certChain := doc.GetCertificate()
	prevDoc := doc
	prevKeyLevel := keyLevel
	for {
		cert, err := VerifyCertificate(certChain, keyLevel)
		if err != nil {
			return false, nil, nil, err
		}

		if subject == nil {
			subject = cert.Subject
		}

		signers = append(signers, cert.Issuer)

		if !cert.AllowedType(prevDoc) {
			return false, nil, nil, fmt.Errorf("Certificate not allowed to sign document of type %s", prevDoc.GetType())
		}

		if !cert.AllowedType(doc) {
			return false, nil, nil, fmt.Errorf("Certificate not allowed to sign document of type %s", doc.GetType())
		}

		if prevKeyLevel == keyLevel {
			prevKeyLevel = cert.KeyLevel
		}
		if cert.KeyLevel > prevKeyLevel {
			return false, nil, nil, errors.New("Not possible to have parent certificate with lower keyLevel than child")
		}

		if cert.Certificate == "" {
			break
		}

		prevDoc = cert
		prevKeyLevel = cert.KeyLevel
		certChain = cert.Certificate
	}

	return true, signers, subject, nil
}

func VerifyCertificate(certificateString string, keyLevel int) (*document.Certificate, error) {
	certChainJWS, err := UnmarshalSignature([]byte(certificateString))
	if err != nil {
		return nil, fmt.Errorf("Invalid certificate JWS")
	}
	if len(certChainJWS.Signatures) < 1 {
		return nil, fmt.Errorf("Invalid header of JWS, does not contain signing key")
	}
	if certChainJWS.Signatures[0].Header.JsonWebKey == nil {
		return nil, fmt.Errorf("Invalid header of JWS, does not contain signing key")
	}

	signingIssuerTP := Thumbprint(certChainJWS.Signatures[0].Header.JsonWebKey)

	payload, err := certChainJWS.Verify(certChainJWS.Signatures[0].Header.JsonWebKey)
	if err != nil {
		return nil, fmt.Errorf("Invalid signature of certificate chain")
	}

	certificate := document.Certificate{}
	err = json.Unmarshal(payload, &certificate)
	if err != nil {
		return nil, fmt.Errorf("Could not unmarshal certificate")
	}

	if certificate.KeyLevel > keyLevel {
		return nil, fmt.Errorf("Key level %d is higher than allowed level of %d", certificate.KeyLevel, keyLevel)
	}

	issuerTP := Thumbprint(certificate.Issuer)
	if issuerTP != signingIssuerTP {
		return nil, fmt.Errorf("Chain was not signed by root key specified in chain")
	}

	if certificate.TTL != 0 && certificate.Timestamp.Add(time.Second*time.Duration(certificate.TTL)).Before(time.Now().UTC()) {
		return nil, fmt.Errorf("Certificate has expired")
	}

	return &certificate, nil
}

func CreateCertificate(issuer, subject *jose.JsonWebKey, keyLevel int, documentTypes []string, ttl int, certificateChain string) (string, error) {
	issuerPK, err := NewPublicKey(issuer)
	if err != nil {
		return "", err
	}

	chain := &document.Certificate{
		Base: document.Base{
			Type:        document.CertificateType,
			Timestamp:   time.Now().UTC(),
			Certificate: certificateChain,
		},
		TTL:           ttl,
		Issuer:        issuerPK,
		Subject:       subject,
		DocumentTypes: documentTypes,
		KeyLevel:      keyLevel,
	}

	signer, err := NewSigner(issuer)
	if err != nil {
		return "", err
	}

	chainBytes, err := json.Marshal(chain)
	if err != nil {
		return "", err
	}

	sig, err := signer.Sign(chainBytes)
	if err != nil {
		return "", err
	}

	sigString, err := sig.CompactSerialize()
	if err != nil {
		return "", err
	}

	return sigString, nil
}
