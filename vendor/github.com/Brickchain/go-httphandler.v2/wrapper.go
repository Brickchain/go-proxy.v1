package httphandler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	logger "github.com/Brickchain/go-logger.v1"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type contextKey string

const requestIDKey = contextKey("requestID")

var dontLogAgents = []string{"GoogleHC", "Go-http-client", "kube-probe"}

// SetDontLogAgents sets which useragents we should not log requests for
func SetDontLogAgents(a []string) {
	dontLogAgents = a
}

// AddRequestID adds the request id to the context
func AddRequestID(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = uuid.Must(uuid.NewV4()).String()
		}
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		h.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}

// GetRequestID is a helper to get the request id from the context
func GetRequestID(ctx context.Context) (requestID string, ok bool) {
	requestID, ok = ctx.Value(requestIDKey).(string)
	return
}

// Wrapper is the struct for holding wrapper related things
type Wrapper struct {
	prod        bool
	middlewares []func(req Request, res Response) (Response, error)
}

// NewWrapper returns a new Wrapper instance
func NewWrapper(prod bool) *Wrapper {
	return &Wrapper{
		prod:        prod,
		middlewares: make([]func(req Request, res Response) (Response, error), 0),
	}
}

// AddMiddleware ...
func (wrapper *Wrapper) AddMiddleware(f func(req Request, res Response) (Response, error)) {
	wrapper.middlewares = append(wrapper.middlewares, f)
}

// Wrap is the main wrapper for making the regular httprouter.Handle type in to our Request/Response types
func (wrapper *Wrapper) Wrap(h interface{}) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		start := time.Now()

		req := newStandardRequest(w, r, p)

		var err error

		payload := make([]byte, 0)
		status := 0

		// Log request when done
		defer func() {
			agent := strings.Split(r.UserAgent(), "/")[0]
			ignore := false
			for _, f := range dontLogAgents {
				if agent == f {
					ignore = true
				}
			}

			if !ignore {
				stop := time.Since(start)
				fields := logger.Fields{
					"status":        status,
					"duration":      float64(stop.Nanoseconds()) / float64(1000),
					"response-size": len(payload),
				}
				req.Log().WithFields(fields).Info(http.StatusText(status))
			}
		}()

		var f func(Request) Response
		switch x := h.(type) {
		case func(http.ResponseWriter, *http.Request, httprouter.Params):
			x(w, r, p)
			return
		case func(http.ResponseWriter, *http.Request, httprouter.Params) error:
			if err := x(w, r, p); err != nil {
				req.Log().Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		case func(Request) Response:
			f = x
		case func(AuthenticatedRequest) Response:
			f = addAuthentication(x)
		case func(OptionalAuthenticatedRequest) Response:
			f = addOptionalAuthentication(x)
		case func(ActionRequest) Response:
			f = parseAction(x)
		default:
			err := errors.New("Unknown type signature")
			req.Log().Error(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Wrap the handler and catch panics
		res := CatchPanic(f, req)

		// Update status values
		status = res.StatusCode()
		req.Log().AddField("status", res.StatusCode())

		switch x := res.(type) {
		case *ErrorResponse:
			fields := logger.Fields{}
			if x.StackTrace() != "" {
				fields["stacktrace"] = x.StackTrace()
			}
			req.Log().WithFields(fields).Error(x.Error())

			msg := x.Msg()
			msg.ErrorID = req.ID()

			// Remove stacktrace if running in prod mode
			if wrapper.prod {
				msg.StackTrace = ""
			}

			payload, err = json.Marshal(msg)
			if err != nil {
				req.Log().Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		case *JsonResponse:
			payload, err = json.Marshal(x.Payload())
			if err != nil {
				req.Log().Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		default:
			switch v := x.Payload().(type) {
			case []byte:
				payload = v
			case string:
				payload = []byte(v)
			case nil:
				payload = nil
			default:
				payload, err = json.Marshal(v)
				if err != nil {
					req.Log().Error(err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		}

		for _, f := range wrapper.middlewares {
			res, err = f(req, res)
			if err != nil {
				req.Log().Error(err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", res.ContentType())

		for key, vals := range res.Header() {
			for _, val := range vals {
				w.Header().Add(key, val)
			}
		}

		w.WriteHeader(status)
		if payload != nil {
			w.Write(payload)
		}
	}
}

// CatchPanic wraps the request execution with code to catch if there is a panic
func CatchPanic(h func(Request) Response, req Request) Response {
	var res Response
	wg := sync.WaitGroup{}

	wg.Add(1)
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				err := fmt.Errorf("Panic: %v", rec)
				st := fmt.Sprintf("Panic: %v\n%v", rec, string(debug.Stack()))
				msg := errResp{
					ErrorMsg:   "An error occured",
					ErrorID:    req.ID(),
					StackTrace: st,
				}

				res = &ErrorResponse{
					StandardResponse: NewStandardResponse(http.StatusInternalServerError, "application/json", nil),
					err:              err,
					msg:              msg,
					stacktrace:       st,
				}

				wg.Done()
			}
		}()
		res = h(req)

		wg.Done()
	}()

	wg.Wait()
	return res
}
