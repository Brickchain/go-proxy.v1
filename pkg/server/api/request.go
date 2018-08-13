package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	httphandler "github.com/Brickchain/go-httphandler.v2"
	proxy "github.com/Brickchain/go-proxy.v1"
	"github.com/Brickchain/go-proxy.v1/pkg/server/clients"
	pubsub "github.com/Brickchain/go-pubsub.v1"
	"github.com/pkg/errors"
	"github.com/ulule/limiter"
)

type RequestController struct {
	domain  string
	clients *clients.ClientService
	pubsub  pubsub.PubSubInterface
	limiter *limiter.Limiter
}

func NewRequestController(domain string, clients *clients.ClientService, pubsub pubsub.PubSubInterface, limiter *limiter.Limiter) *RequestController {
	return &RequestController{
		domain:  domain,
		clients: clients,
		pubsub:  pubsub,
		limiter: limiter,
	}
}

func (s *RequestController) Handle(req httphandler.Request) httphandler.Response {
	clientID := req.Params().ByName("clientID")

	// If domain is set and the request was for a host that has our domain as suffix we will strip away the domain and use the rest as the clientID
	if s.domain != "" {
		host := req.OriginalRequest().Host
		if strings.HasSuffix(host, "."+s.domain) {
			clientID = strings.Replace(host, "."+s.domain, "", 1)
		}
	}

	_, err := s.clients.Get(clientID)
	if err != nil {
		return httphandler.NewErrorResponse(http.StatusBadGateway, errors.Wrap(err, "failed to get client"))
	}

	limit, err := s.limiter.Get(req.Context(), clientID)
	if err != nil {
		return httphandler.NewErrorResponse(http.StatusInternalServerError, errors.Wrap(err, "failed to get limit"))
	}

	if limit.Reached {
		return httphandler.NewEmptyResponse(http.StatusTooManyRequests)
	}

	headers := make(map[string]string)
	for k, v := range req.Header() {
		headers[k] = v[0]
	}
	// headers["Host"] = req.OriginalRequest().Host

	msg := proxy.NewHttpRequest(req.Params().ByName("filepath"))
	msg.Headers = headers
	msg.Method = req.OriginalRequest().Method

	data, err := ioutil.ReadAll(io.LimitReader(req.OriginalRequest().Body, 1024*500))
	if err == nil {
		if err := req.OriginalRequest().Body.Close(); err != nil {
			return httphandler.NewErrorResponse(http.StatusInternalServerError, errors.Wrap(err, "failed to close body"))
		}
		msg.Body = base64.StdEncoding.EncodeToString(data)
	}

	body, _ := json.Marshal(msg)

	topic := fmt.Sprintf("/proxy/responses/%s", msg.ID)
	sub, err := s.pubsub.Subscribe(msg.ID, topic)
	if err != nil {
		return httphandler.NewErrorResponse(http.StatusInternalServerError, errors.Wrap(err, "failed to push message"))
	}
	// defer sub.Stop(time.Second * 1)
	// defer c.pubsub.DeleteTopic(topic)

	if err := s.pubsub.Publish(fmt.Sprintf("/proxy/connections/%s", clientID), string(body)); err != nil {
		return httphandler.NewErrorResponse(http.StatusInternalServerError, errors.Wrap(err, "failed to send"))
	}

	respString, i := sub.Pull(time.Second * 10)
	if i == pubsub.TIMEOUT {
		return httphandler.NewErrorResponse(http.StatusGatewayTimeout, errors.New("timeout"))
	}
	if i == pubsub.ERROR {
		return httphandler.NewErrorResponse(http.StatusInternalServerError, errors.New("error"))
	}

	resp := &proxy.HttpResponse{}
	if err := json.Unmarshal([]byte(respString), &resp); err != nil {
		return httphandler.NewErrorResponse(http.StatusInternalServerError, errors.Wrap(err, "failed to unmarshal response"))
	}

	respBody, err := base64.StdEncoding.DecodeString(resp.Body)
	if err != nil {
		return httphandler.NewErrorResponse(http.StatusInternalServerError, errors.Wrap(err, "failed to decode response body"))
	}

	r := httphandler.NewStandardResponse(resp.Status, resp.ContentType, respBody)

	for k, v := range resp.Headers {
		// if k != "Content-Type" && k != "Access-Control-Allow-Origin" && k != "Server" {
		r.Header().Set(k, v)
		// }
	}

	return r
}
