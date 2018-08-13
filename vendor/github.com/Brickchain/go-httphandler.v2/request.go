package httphandler

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/url"

	logger "github.com/Brickchain/go-logger.v1"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Request is the base interface for HTTP requests
type Request interface {
	ID() string
	OriginalRequest() *http.Request
	Response() http.ResponseWriter
	Context() context.Context
	Header() http.Header
	Params() httprouter.Params
	URL() *url.URL
	Body() ([]byte, error)
	Log() *logger.Entry
}

// standardRequest implements the Request interface
type standardRequest struct {
	id     string
	req    *http.Request
	res    http.ResponseWriter
	ctx    context.Context
	header http.Header
	params httprouter.Params
	url    *url.URL
	log    *logger.Entry
}

// newStandardRequest returns a new standardRequest object with the request-id and logger setup
func newStandardRequest(w http.ResponseWriter, r *http.Request, params httprouter.Params) *standardRequest {
	reqID := uuid.Must(uuid.NewV4()).String()
	fields := logger.Fields{
		"request-id":   reqID,
		"host":         r.Host,
		"proto":        r.Proto,
		"method":       r.Method,
		"request":      r.RequestURI,
		"remote":       r.RemoteAddr,
		"referer":      r.Referer(),
		"user-agent":   r.UserAgent(),
		"request-size": r.ContentLength,
	}
	l := logger.WithFields(fields)
	return &standardRequest{
		id:     reqID,
		req:    r,
		res:    w,
		ctx:    r.Context(),
		header: r.Header,
		params: params,
		url:    r.URL,
		log:    l,
	}
}

func (r *standardRequest) setLogger(l *logger.Entry) {
	r.log = l
}

// ID returns the request ID
func (r *standardRequest) ID() string {
	return r.id
}

// OriginalRequest returns the original request object
func (r *standardRequest) OriginalRequest() *http.Request {
	return r.req
}

// Response returns the original response object
func (r *standardRequest) Response() http.ResponseWriter {
	return r.res
}

// Context returns the context object
func (r *standardRequest) Context() context.Context {
	return r.ctx
}

// Header returns the Header object
func (r *standardRequest) Header() http.Header {
	return r.header
}

// Params returns the Params object from httprouter
func (r *standardRequest) Params() httprouter.Params {
	return r.params
}

// URL returns the request URL object
func (r *standardRequest) URL() *url.URL {
	return r.url
}

// Body reads the body payload from the request as a byte slice
func (r *standardRequest) Body() ([]byte, error) {
	body, err := ioutil.ReadAll(r.req.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read body")
	}
	if err = r.req.Body.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to close body")
	}

	return body, nil
}

// Log returns a logger with the request context fields set
func (r *standardRequest) Log() *logger.Entry {
	return r.log
}
