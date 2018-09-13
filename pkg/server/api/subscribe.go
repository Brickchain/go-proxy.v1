package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	crypto "github.com/Brickchain/go-crypto.v2"
	document "github.com/Brickchain/go-document.v2"
	logger "github.com/Brickchain/go-logger.v1"
	proxy "github.com/Brickchain/go-proxy.v1"
	"github.com/Brickchain/go-proxy.v1/pkg/server"
	"github.com/Brickchain/go-proxy.v1/pkg/server/clients"
	pubsub "github.com/Brickchain/go-pubsub.v1"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	jose "gopkg.in/square/go-jose.v1"
)

type SubscribeController struct {
	domain  string
	clients *clients.ClientService
	pubsub  pubsub.PubSubInterface
}

func NewSubscribeController(domain string, clients *clients.ClientService, pubsub pubsub.PubSubInterface) *SubscribeController {
	return &SubscribeController{
		domain:  domain,
		clients: clients,
		pubsub:  pubsub,
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (c *SubscribeController) SubscribeHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	clients := make([]*server.Client, 0)

	respHeaders := make(http.Header)
	respHeaders.Add("Access-Control-Allow-Origin", r.Header.Get("Origin"))
	conn, err := upgrader.Upgrade(w, r, respHeaders)
	if err != nil {
		http.Error(w, errors.Wrap(err, "failed to upgrade to websocket").Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	lock := sync.Mutex{}
	write := func(msg []byte) error {
		lock.Lock()
		defer lock.Unlock()

		return conn.WriteMessage(websocket.TextMessage, msg)
	}

	wg := sync.WaitGroup{}

	addClient := func(r *proxy.RegistrationRequest) {
		key, err := parseMandateToken(r.MandateToken)
		if err != nil {
			write([]byte(err.Error()))
			logger.Error(err)
			return
		}

		client := server.NewClient(key, r.Session)
		if err := c.clients.Set(client); err != nil {
			write([]byte(err.Error()))
			logger.Error(err)
			return
		}

		logger.Debugf("Adding client %s", client.ID)
		defer wg.Done()

		clients = append(clients, client)

		sub, err := c.pubsub.Subscribe("proxy", fmt.Sprintf("/proxy/connections/%s", client.ID))
		if err != nil {
			logger.Error(errors.Wrap(err, "failed to push message"))
			return
		}
		defer sub.Stop(time.Second * 1)

		res := proxy.NewRegistrationResponse(r.ID, client.ID)
		if c.domain != "" {
			res.Hostname = fmt.Sprintf("%s.%s", client.ID, c.domain)
		}
		resBytes, _ := json.Marshal(res)
		if err = write([]byte(resBytes)); err != nil {
			cancel()
			logger.Error(err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 10):
				if err = write([]byte("{\"@type\":\"https://proxy.brickchain.com/v1/ping.json\"}\n")); err != nil {
					logger.Error(err)
					cancel()
					return
				}
			case msg := <-sub.Chan():
				if err = write([]byte(msg)); err != nil {
					logger.Error(err)
					cancel()
					return
				}

				if err := c.clients.RenewTTL(client.ID); err != nil {
					logger.Errorf("failed to renew TTL for %s: %s", client.ID, err)
				}
			}
		}
	}

	go func() {
		<-time.After(time.Second * 10)
		if len(clients) < 1 {
			logger.Warn("Not authenticated after 10 seconds, dropping connection")
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, body, err := conn.ReadMessage()
				if err != nil {
					fmt.Printf("got error while reading message: %s\n", err)
					// close(done)
					return
				}

				go func() {
					wg.Add(1)
					defer wg.Done()
					docType, err := document.GetType(body)
					if err != nil {
						logger.Error("failed to get type of document: ", err)
						return
					}

					switch docType {
					case proxy.SchemaBase + "/registration-request.json":
						r := &proxy.RegistrationRequest{}
						if err := json.Unmarshal(body, &r); err != nil {
							logger.Error("failed to unmarshal registration-request: ", err)
							return
						}

						wg.Add(1)
						go addClient(r)
						return

					case proxy.SchemaBase + "/http-response.json":
						r := &proxy.HttpResponse{}
						if len(body) > 1024*500 {
							r.Status = http.StatusBadGateway
						} else {
							if err := json.Unmarshal(body, &r); err != nil {
								fmt.Printf("could not unmarshal message: %s\n", err)
								return
							}
						}
						if r != nil {
							if err := c.pubsub.Publish(fmt.Sprintf("/proxy/responses/%s", r.ID), string(body)); err != nil {
								fmt.Printf("could not publish message: %s\n", err)
							}
						}
					case proxy.SchemaBase + "/ws-response.json":
						r := &proxy.WSResponse{}
						if err := json.Unmarshal(body, &r); err != nil {
							fmt.Printf("could not unmarshal message: %s\n", err)
							return
						}
						if err := c.pubsub.Publish(fmt.Sprintf("/proxy/websocket/%s", r.ID), string(body)); err != nil {
							fmt.Printf("could not publish message: %s\n", err)
						}
					case proxy.SchemaBase + "/ws-message.json":
						r := &proxy.WSMessage{}
						if err := json.Unmarshal(body, &r); err != nil {
							fmt.Printf("could not unmarshal message: %s\n", err)
							return
						}
						if err := c.pubsub.Publish(fmt.Sprintf("/proxy/websocket/%s", r.ID), string(body)); err != nil {
							fmt.Printf("could not publish message: %s\n", err)
						}
					case proxy.SchemaBase + "/ws-teardown.json":
						r := &proxy.WSTeardown{}
						if err := json.Unmarshal(body, &r); err != nil {
							fmt.Printf("could not unmarshal message: %s\n", err)
							return
						}
						if err := c.pubsub.Publish(fmt.Sprintf("/proxy/websocket/%s", r.ID), string(body)); err != nil {
							fmt.Printf("could not publish message: %s\n", err)
						}
					case proxy.SchemaBase + "/disconnect.json":
						cancel()
						return
					}

				}()
			}
			time.Sleep(time.Millisecond * 1)
		}
	}()

	wg.Wait()

}

func parseMandateToken(tokenString string) (*jose.JsonWebKey, error) {
	tokenJWS, err := crypto.UnmarshalSignature([]byte(tokenString))
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal JWS")
	}

	if len(tokenJWS.Signatures) < 1 || tokenJWS.Signatures[0].Header.JsonWebKey == nil {
		return nil, errors.New("no jwk in token")
	}

	payload, err := tokenJWS.Verify(tokenJWS.Signatures[0].Header.JsonWebKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to verify token")
	}

	token := &document.MandateToken{}
	err = json.Unmarshal(payload, &token)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal token")
	}

	if token.Timestamp.Add(time.Second * time.Duration(token.TTL)).Before(time.Now().UTC()) {
		return nil, errors.New("Token has expired")
	}

	if !strings.HasPrefix(token.URI, viper.GetString("base")) {
		return nil, errors.New("Token not for this endpoint")
	}

	return tokenJWS.Signatures[0].Header.JsonWebKey, nil
}
