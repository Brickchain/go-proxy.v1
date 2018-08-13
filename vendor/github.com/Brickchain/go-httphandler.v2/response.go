package httphandler

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/pkg/errors"
)

// Response is the base response interface type
type Response interface {
	StatusCode() int
	ContentType() string
	Header() http.Header
	Payload() interface{}
}

// StandardResponse implements the Response interface
type StandardResponse struct {
	statusCode  int
	contentType string
	header      http.Header
	payload     interface{}
}

// NewStandardResponse returns a new StandardResponse
func NewStandardResponse(statusCode int, contentType string, payload interface{}) *StandardResponse {
	return &StandardResponse{
		statusCode:  statusCode,
		contentType: contentType,
		payload:     payload,
		header:      http.Header{},
	}
}

// StatusCode returns the response status code
func (r *StandardResponse) StatusCode() int {
	return r.statusCode
}

// ContentType returns the content type of the response
func (r *StandardResponse) ContentType() string {
	return r.contentType
}

// Header returns the header object for the response
func (r *StandardResponse) Header() http.Header {
	return r.header
}

// Payload returns the response payload
func (r *StandardResponse) Payload() interface{} {
	return r.payload
}

// ErrorResponse extends the StandardResponse with fields for responding with and error
type ErrorResponse struct {
	*StandardResponse
	err        error
	msg        errResp
	stacktrace string
}

// Error returns the error message
func (e *ErrorResponse) Error() error {
	return e.err
}

// Msg returns the errResp object that we can send back over the HTTP socket
func (e *ErrorResponse) Msg() errResp {
	return e.msg
}

// StackTrace returns the stringified stacktrace of the error/panic (if exists)
func (e *ErrorResponse) StackTrace() string {
	return e.stacktrace
}

type errResp struct {
	ErrorMsg   string `json:"error_message"`
	ErrorID    string `json:"error_id"`
	StackTrace string `json:"stacktrace,omitempty"`
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// NewErrorResponse returns a new ErrorResponse with a status code and error set
// if err contains a stacktrace (if created with the errors package) we will stringify it and attach to the response.
func NewErrorResponse(statusCode int, err error) *ErrorResponse {
	st := ""

	t, ok := err.(stackTracer)
	if ok {
		st = fmt.Sprintf("%s:%+v", err.Error(), t.StackTrace())
	} else {
		st = string(debug.Stack())
	}

	m := errResp{
		ErrorMsg:   err.Error(),
		StackTrace: st,
	}
	r := &ErrorResponse{
		StandardResponse: NewStandardResponse(statusCode, "application/json", nil),
		err:              err,
		msg:              m,
		stacktrace:       st,
	}
	// r.err = err

	return r
}

// JsonResponse is a StandardResponse but used as a shortcut for not having to specify the content type
type JsonResponse struct {
	*StandardResponse
}

// NewJsonResponse returns a new JsonResponse
func NewJsonResponse(statusCode int, payload interface{}) *JsonResponse {
	return &JsonResponse{NewStandardResponse(statusCode, "application/json", payload)}
}

// EmptyResponse is a StandardResponse but without payload
type EmptyResponse struct {
	*StandardResponse
}

// NewEmptyResponse returns a new EmptyResponse with the status code set
func NewEmptyResponse(statusCode int) *EmptyResponse {
	return &EmptyResponse{NewStandardResponse(statusCode, "", nil)}
}
