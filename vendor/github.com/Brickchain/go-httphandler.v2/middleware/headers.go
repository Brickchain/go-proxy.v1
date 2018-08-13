package middleware

import (
	"net/http"
)

// ResponseHeader adds a header to each response
func ResponseHeader(h http.Handler, key string, value string) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(key, value)
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}
