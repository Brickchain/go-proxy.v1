package httphandler

import (
	"encoding/json"
	"net/http"

	"github.com/Brickchain/go-crypto.v2"
	"github.com/Brickchain/go-document.v2"
	"github.com/pkg/errors"
	jose "gopkg.in/square/go-jose.v1"
)

// ActionRequest is describes a Brickchain Action being posted
type ActionRequest interface {
	Request
	Mandates() []AuthenticatedMandate
	Action() *document.Action
	Key() *jose.JsonWebKey
	KeyLevel() int
}

// authenticatedActionRequest is the standard implementation of an AuthenticatedRequest
type authenticatedActionRequest struct {
	Request
	mandates []AuthenticatedMandate
	action   *document.Action
	key      *jose.JsonWebKey
	keyLevel int
}

func (r *authenticatedActionRequest) Mandates() []AuthenticatedMandate {
	return r.mandates
}

func (r *authenticatedActionRequest) Action() *document.Action {
	return r.action
}

func (r *authenticatedActionRequest) Key() *jose.JsonWebKey {
	return r.key
}

func (r *authenticatedActionRequest) KeyLevel() int {
	return r.keyLevel
}

func parseAction(h func(ActionRequest) Response) func(Request) Response {
	return func(req Request) Response {
		body, err := req.Body()
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to read message body"))
		}

		sig, err := crypto.UnmarshalSignature(body)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to unmarshal JWS"))
		}

		if len(sig.Signatures) < 1 {
			return NewErrorResponse(http.StatusBadRequest, errors.New("No signatures"))
		}

		userPK := sig.Signatures[0].Header.JsonWebKey
		payload, err := sig.Verify(userPK)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to verify signature"))
		}

		action := &document.Action{}
		err = json.Unmarshal(payload, &action)
		if err != nil {
			return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to unmarshal JWS payload"))
		}

		var actionCertificate *document.Certificate
		if action.Certificate != "" {
			actionCertificate, err = crypto.VerifyCertificate(action.Certificate, 10000)
			if err != nil {
				return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to verify certificate chain"))
			}
			if crypto.Thumbprint(userPK) != crypto.Thumbprint(actionCertificate.Subject) {
				return NewErrorResponse(http.StatusBadRequest, errors.New("Action signature mismatch"))
			}
		} else {
			actionCertificate = &document.Certificate{
				Issuer:        userPK,
				Subject:       userPK,
				DocumentTypes: []string{"*"},
				KeyLevel:      0,
			}
		}

		mandates := make([]AuthenticatedMandate, 0)
		for i, m := range action.Mandates {
			mandateSig, err := crypto.UnmarshalSignature([]byte(m))
			if err != nil {
				return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to unmarshal mandate JWS"))
			}

			mandatePK := mandateSig.Signatures[0].Header.JsonWebKey

			mandateBytes, err := mandateSig.Verify(mandatePK)
			if err != nil {
				return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to verify mandate signature"))
			}

			mandate := &document.Mandate{}
			err = json.Unmarshal(mandateBytes, &mandate)
			if err != nil {
				return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to unmarshal mandate"))
			}

			signingKey := mandateSig.Signatures[0].Header.JsonWebKey

			var mandateCertificate *document.Certificate
			if mandate.Certificate != "" {
				mandateCertificate, err = crypto.VerifyCertificate(mandate.Certificate, 1) // mandates are signed by the realm root key or the realm signing key
				if err != nil {
					return NewErrorResponse(http.StatusBadRequest, errors.Wrap(err, "failed to verify mandate certificate chain"))
				}
				if crypto.Thumbprint(mandateCertificate.Subject) != crypto.Thumbprint(mandatePK) {
					return NewErrorResponse(http.StatusBadRequest, errors.New("Signer of mandate is not same as subKey in chain"))
				}

				signingKey = mandateCertificate.Issuer
			}

			if crypto.Thumbprint(actionCertificate.Issuer) != crypto.Thumbprint(mandate.Recipient) {
				if len(action.Mandates) > i {
					action.Mandates = append(action.Mandates[:i], action.Mandates[i+1:]...)
				} else {
					action.Mandates = action.Mandates[:i]
				}

				continue
			}

			mandates = append(mandates, AuthenticatedMandate{
				Mandate: mandate,
				Signer:  signingKey,
			})
		}

		r := &authenticatedActionRequest{
			Request:  req,
			mandates: mandates,
			action:   action,
			key:      userPK,
			keyLevel: actionCertificate.KeyLevel,
		}

		return h(r)
	}
}
