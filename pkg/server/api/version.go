package api

import (
	"os"
	"path"

	httphandler "github.com/Brickchain/go-httphandler.v2"
	"github.com/Brickchain/go-proxy.v1/pkg/version"

	"net/http"
)

// Version returns the servers version
func Version(req httphandler.Request) httphandler.Response {
	return httphandler.NewStandardResponse(http.StatusOK, "text/plain", path.Base(os.Args[0])+"/"+version.Version)
}
