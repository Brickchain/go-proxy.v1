package httphandler

import (
	"net/http"
	"os"

	"github.com/Brickchain/go-httphandler.v2/middleware"
	gorillaHandlers "github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
)

var exposedHeaders = []string{"Content-Language", "Content-Type", "X-Boot-Mode"}
var allowedHeaders = []string{"Accept", "Accept-Language", "Content-Language", "Content-Type", "Origin", "Authorization", "X-Auth-Token"}
var allowedMethods = []string{"GET", "POST", "PUT", "DELETE"}

// SetExposedHeaders sets the headers we expose in CORS
func SetExposedHeaders(h []string) {
	exposedHeaders = h
}

// SetAllowedHeaders sets the headers we allow in CORS
func SetAllowedHeaders(h []string) {
	allowedHeaders = h
}

// SetAllowedMethods sets the methods we allow in CORS
func SetAllowedMethods(m []string) {
	allowedMethods = m
}

// NewRouter returns a new httprouter.Router object
func NewRouter() *httprouter.Router {
	return httprouter.New()
}

// LoadMiddlewares adds the middlewares for CORS and Server and proxy headers
func LoadMiddlewares(router *httprouter.Router, version string) http.Handler {
	headersOk := gorillaHandlers.AllowedHeaders(allowedHeaders)
	exposedHeadersOK := gorillaHandlers.ExposedHeaders(exposedHeaders)
	methodsOk := gorillaHandlers.AllowedMethods(allowedMethods)
	handler := gorillaHandlers.CORS(headersOk, methodsOk, exposedHeadersOK)(router)
	handler = middleware.ResponseHeader(handler, "Server", os.Args[0]+"/"+version)
	handler = gorillaHandlers.ProxyHeaders(handler)

	return handler
}
