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

	document "github.com/Brickchain/go-document.v2"
	logger "github.com/Brickchain/go-logger.v1"
	proxy "github.com/Brickchain/go-proxy.v1"
	"github.com/Brickchain/go-proxy.v1/pkg/server/clients"
	pubsub "github.com/Brickchain/go-pubsub.v1"
	"github.com/julienschmidt/httprouter"
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

func (s *RequestController) Handle(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	clientID := params.ByName("clientID")

	// If domain is set and the request was for a host that has our domain as suffix we will strip away the domain and use the rest as the clientID
	if s.domain != "" {
		host := r.Host
		if strings.HasSuffix(host, "."+s.domain) {
			clientID = strings.Replace(host, "."+s.domain, "", 1)
		}
	}

	_, err := s.clients.Get(clientID)
	if err != nil {
		http.Error(w, errors.Wrap(err, "failed to get client").Error(), http.StatusBadGateway)
		return
	}

	limit, err := s.limiter.Get(r.Context(), clientID)
	if err != nil {
		http.Error(w, errors.Wrap(err, "failed to get limit").Error(), http.StatusInternalServerError)
		return
	}

	if limit.Reached {
		http.Error(w, "Too many requests", http.StatusTooManyRequests)
		return
	}

	if strings.ToLower(r.Header.Get("Connection")) == "upgrade" && strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
		respHeaders := make(http.Header)
		respHeaders.Add("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		conn, err := upgrader.Upgrade(w, r, respHeaders)
		if err != nil {
			http.Error(w, errors.Wrap(err, "failed to upgrade to websocket").Error(), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		msg := proxy.NewWSRequest(params.ByName("filepath"))
		msg.Headers = make(map[string]string)
		for k, v := range r.Header {
			switch strings.ToUpper(k) {
			case "CONNECTION":
			case "UPGRADE":
			case "SEC-WEBSOCKET-KEY":
			case "SEC-WEBSOCKET-VERSION":
			case "SEC-WEBSOCKET-EXTENSIONS":
			default:
				msg.Headers[k] = v[0]
			}
		}

		msgSub, err := s.pubsub.Subscribe(msg.ID, fmt.Sprintf("/proxy/websocket/%s", msg.ID))
		if err != nil {
			http.Error(w, errors.Wrap(err, "failed to setup msg listener").Error(), http.StatusInternalServerError)
			return
		}
		defer msgSub.Stop(time.Second * 1)

		done := make(chan bool)
		teardown := make(chan bool)
		go func() {
			defer func() {
				teardown <- true
			}()
			for {
				select {
				case <-done:
					return
				default:
					mString, i := msgSub.Pull(time.Second * 10)
					if i == pubsub.ERROR {
						return
					}
					if i == pubsub.TIMEOUT {
						time.Sleep(time.Millisecond * 10)
						continue
					}

					b := document.Base{}
					if err := json.Unmarshal([]byte(mString), &b); err != nil {
						logger.Error(err)
						continue
					}

					switch b.Type {
					case proxy.SchemaBase + "/ws-response.json":
						m := proxy.WSResponse{}
						if err := json.Unmarshal([]byte(mString), &m); err != nil {
							logger.Error(err)
							continue
						}

						if !m.OK {
							http.Error(w, m.Error, http.StatusInternalServerError)
							conn.Close()
							return
						}
					case proxy.SchemaBase + "/ws-message.json":
						m := proxy.WSMessage{}
						if err := json.Unmarshal([]byte(mString), &m); err != nil {
							logger.Error(err)
							continue
						}

						if err := conn.WriteMessage(m.MessageType, []byte(m.Body)); err != nil {
							logger.Error(err)
							return
						}
					case proxy.SchemaBase + "/ws-teardown.json":
						logger.Info("Shutting down connection")
						conn.Close()
						return
					}

				}

				time.Sleep(time.Millisecond * 10)
			}
		}()

		body, _ := json.Marshal(msg)

		if err := s.pubsub.Publish(fmt.Sprintf("/proxy/connections/%s", clientID), string(body)); err != nil {
			http.Error(w, errors.Wrap(err, "failed to send").Error(), http.StatusInternalServerError)
			return
		}

		defer func() {
			done <- true

			m := proxy.NewWSTeardown(msg.ID)
			b, _ := json.Marshal(m)

			if err := s.pubsub.Publish(fmt.Sprintf("/proxy/connections/%s", clientID), string(b)); err != nil {
				logger.Error(err)
				return
			}
		}()
		for {
			select {
			case <-teardown:
				return
			default:
				typ, body, err := conn.ReadMessage()
				if err != nil {
					logger.Error("got error while reading message", err)
					return
				}

				m := proxy.NewWSMessage(msg.ID)
				m.MessageType = typ
				m.Body = string(body)

				b, _ := json.Marshal(m)

				if err := s.pubsub.Publish(fmt.Sprintf("/proxy/connections/%s", clientID), string(b)); err != nil {
					http.Error(w, errors.Wrap(err, "failed to send").Error(), http.StatusInternalServerError)
					return
				}
			}

			time.Sleep(time.Millisecond * 10)
		}
	} else {

		msg := proxy.NewHttpRequest(params.ByName("filepath"))
		msg.Headers = make(map[string]string)
		for k, v := range r.Header {
			msg.Headers[k] = v[0]
		}
		msg.Method = r.Method

		data, err := ioutil.ReadAll(io.LimitReader(r.Body, 1024*500))
		if err == nil {
			if err := r.Body.Close(); err != nil {
				http.Error(w, errors.Wrap(err, "failed to close body").Error(), http.StatusInternalServerError)
				return
			}
			msg.Body = base64.StdEncoding.EncodeToString(data)
		}

		body, _ := json.Marshal(msg)

		topic := fmt.Sprintf("/proxy/responses/%s", msg.ID)
		sub, err := s.pubsub.Subscribe(msg.ID, topic)
		if err != nil {
			http.Error(w, errors.Wrap(err, "failed to push message").Error(), http.StatusInternalServerError)
			return
		}
		// defer sub.Stop(time.Second * 1)
		// defer c.pubsub.DeleteTopic(topic)

		if err := s.pubsub.Publish(fmt.Sprintf("/proxy/connections/%s", clientID), string(body)); err != nil {
			http.Error(w, errors.Wrap(err, "failed to send").Error(), http.StatusInternalServerError)
			return
		}

		respString, i := sub.Pull(time.Second * 10)
		if i == pubsub.TIMEOUT {
			http.Error(w, "timeout", http.StatusGatewayTimeout)
			return
		}
		if i == pubsub.ERROR {
			http.Error(w, "error", http.StatusInternalServerError)
			return
		}

		resp := &proxy.HttpResponse{}
		if err := json.Unmarshal([]byte(respString), &resp); err != nil {
			logger.Error(errors.Wrap(err, "failed to unmarshal response"))
			http.Error(w, errors.Wrap(err, "failed to unmarshal response").Error(), http.StatusInternalServerError)
			return
		}

		respBody, err := base64.StdEncoding.DecodeString(resp.Body)
		if err != nil {
			logger.Error(errors.Wrap(err, "failed to decode response body"))
			http.Error(w, errors.Wrap(err, "failed to decode response body").Error(), http.StatusInternalServerError)
			return
		}

		for k, v := range resp.Headers {
			w.Header().Set(k, v)
		}

		w.WriteHeader(resp.Status)
		w.Write(respBody)

	}
}
