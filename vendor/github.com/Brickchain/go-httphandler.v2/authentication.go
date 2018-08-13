package httphandler

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Brickchain/go-crypto.v2"
	"github.com/Brickchain/go-document.v2"
	logger "github.com/Brickchain/go-logger.v1"
	"github.com/pkg/errors"
	jose "gopkg.in/square/go-jose.v1"
)

// AuthenticatedRequest extends the regular Request type with a Mandates() function that returns the mandates used in this request
type AuthenticatedRequest interface {
	Request
	Mandates() []AuthenticatedMandate
	Key() *jose.JsonWebKey
	URI() string
}

// AuthenticatedRequest extends the regular Request type with a Mandates() function that returns the mandates used in this request
type OptionalAuthenticatedRequest interface {
	Request
	Mandates() []AuthenticatedMandate
	Key() *jose.JsonWebKey
	URI() string
}

// AuthenticatedMandate holds the verified mandate and the signer of that mandate
type AuthenticatedMandate struct {
	Mandate *document.Mandate
	Signer  *jose.JsonWebKey
}

// authenticatedMandateRequest is the standard implementation of an AuthenticatedRequest
type authenticatedMandateRequest struct {
	Request
	mandates []AuthenticatedMandate
	key      *jose.JsonWebKey
	uri      string
}

// Mandates returns the mandates that we could validate for this request
func (r *authenticatedMandateRequest) Mandates() []AuthenticatedMandate {
	return r.mandates
}

// Key returns the key of the request sender
func (r *authenticatedMandateRequest) Key() *jose.JsonWebKey {
	return r.key
}

// URI returns the URI parameter from the MandateToken
func (r *authenticatedMandateRequest) URI() string {
	return r.uri
}

// addAuthentication wraps a handler and adds the authentication information
func addAuthentication(h func(AuthenticatedRequest) Response) func(Request) Response {
	return func(req Request) Response {
		userKey, token, err := parseMandateToken(req)
		if err != nil {
			return err
		}

		mandates, err := parseMandates(token)
		if err != nil {
			return err
		}

		r := &authenticatedMandateRequest{
			Request:  req,
			mandates: mandates,
			key:      userKey,
			uri:      token.URI,
		}

		return h(r)
	}
}

// addOptionalAuthentication wraps a handler and adds the authentication information, if there is any
func addOptionalAuthentication(h func(OptionalAuthenticatedRequest) Response) func(Request) Response {
	return func(req Request) Response {
		mandates := make([]AuthenticatedMandate, 0)

		userKey, token, err := parseMandateToken(req)
		if err != nil {
			return err
		}

		mandates, err = parseMandates(token)
		if err != nil {
			return err
		}

		r := &authenticatedMandateRequest{
			Request:  req,
			mandates: mandates,
			key:      userKey,
		}

		if token != nil {
			r.uri = token.URI
		}

		return h(r)
	}
}

func parseMandateToken(req Request) (*jose.JsonWebKey, *document.MandateToken, Response) {
	a := req.Header().Get("Authorization")

	var l = strings.Split(a, " ")

	if len(l) < 2 {
		return nil, nil, NewErrorResponse(http.StatusBadRequest, errors.New("broken auth header"))
	}

	if strings.ToUpper(l[0]) != "MANDATE" {
		return nil, nil, NewErrorResponse(http.StatusBadRequest, errors.New("unknown auth method"))
	}

	tokenJWS, err := crypto.UnmarshalSignature([]byte(l[1]))
	if err != nil {
		return nil, nil, NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to unmarshal JWS"))
	}

	if len(tokenJWS.Signatures) < 1 || tokenJWS.Signatures[0].Header.JsonWebKey == nil {
		return nil, nil, NewErrorResponse(http.StatusBadRequest, errors.New("no jwk in token"))
	}

	payload, err := tokenJWS.Verify(tokenJWS.Signatures[0].Header.JsonWebKey)
	if err != nil {
		return nil, nil, NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to verify token"))
	}

	token := &document.MandateToken{}
	err = json.Unmarshal(payload, &token)
	if err != nil {
		return nil, nil, NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to unmarshal token"))
	}

	userKey := tokenJWS.Signatures[0].Header.JsonWebKey

	if token.Timestamp.Add(time.Second * time.Duration(token.TTL)).Before(time.Now().UTC()) {
		return nil, nil, NewErrorResponse(http.StatusBadRequest, errors.New("Token has expired"))
	}

	if token.Certificate != "" {
		certChain, err := crypto.VerifyCertificate(token.Certificate, 100)
		if err != nil {
			return nil, nil, NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to verify certificate chain in mandate"))
		}

		userKey = certChain.Issuer
	}

	return userKey, token, nil
}

func parseMandates(token *document.MandateToken) ([]AuthenticatedMandate, Response) {
	mandates := make([]AuthenticatedMandate, 0)

	for _, mandateString := range token.Mandates {
		mandateJWS, err := crypto.UnmarshalSignature([]byte(mandateString))
		if err != nil {
			logger.Debug(errors.Wrap(err, "failed to unmarshal mandate"))
			continue
		}

		if len(mandateJWS.Signatures) < 1 {
			logger.Debug(errors.New("No signers of mandate"))
			continue
		}

		mandatePayload, err := mandateJWS.Verify(mandateJWS.Signatures[0].Header.JsonWebKey)
		if err != nil {
			logger.Debug(errors.Wrap(err, "failed to verify mandate signature"))
			continue
		}

		var mandate *document.Mandate
		err = json.Unmarshal(mandatePayload, &mandate)
		if err != nil {
			logger.Debug(errors.Wrap(err, "failed to unmarshal mandate"))
			continue
		}

		if mandate.ValidFrom == nil || mandate.Timestamp.After(time.Now().UTC()) {
			logger.Debug(errors.New("Mandate is not yet valid"))
			continue
		}

		if mandate.ValidFrom != nil && mandate.ValidFrom.After(time.Now().UTC()) {
			logger.Debug(errors.New("Mandate is not yet valid"))
			continue
		}

		if mandate.ValidUntil != nil && mandate.ValidUntil.Before(time.Now().UTC()) {
			logger.Debug("Mandate has expired")
			continue
		}

		signingKey := mandateJWS.Signatures[0].Header.JsonWebKey

		if mandate.GetCertificate() != "" {
			chain, err := crypto.VerifyCertificate(mandate.GetCertificate(), 10)
			if err != nil {
				logger.Debug(errors.Wrap(err, "could not verify certificate chain"))
				continue
			}

			signingKey = chain.Issuer
		}

		mandates = append(mandates, AuthenticatedMandate{
			Mandate: mandate,
			Signer:  signingKey,
		})
	}

	return mandates, nil
}
