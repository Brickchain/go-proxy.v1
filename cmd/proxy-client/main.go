package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	crypto "github.com/Brickchain/go-crypto.v2"
	logger "github.com/Brickchain/go-logger.v1"
	"github.com/Brickchain/go-proxy.v1/pkg/client"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	jose "gopkg.in/square/go-jose.v1"
)

func main() {
	_ = godotenv.Load(".env")
	viper.AutomaticEnv()
	viper.SetDefault("log_formatter", "text")
	viper.SetDefault("log_level", "info")
	viper.SetDefault("local", "http://localhost:8080")
	viper.SetDefault("local_url", "")
	viper.SetDefault("proxy_endpoint", "https://proxy.svc.integrity.app")
	viper.SetDefault("key", "./proxy-client.pem")
	viper.SetDefault("state", "")

	logger.SetOutput(os.Stdout)
	logger.SetFormatter(viper.GetString("log_formatter"))
	logger.SetLevel(viper.GetString("log_level"))

	var key *jose.JsonWebKey
	_, err := os.Stat(viper.GetString("key"))
	if err != nil {
		key, err = crypto.NewKey()
		if err != nil {
			logger.Fatal(err)
		}

		kb, err := crypto.MarshalToPEM(key)
		if err != nil {
			logger.Fatal(err)
		}

		if err := ioutil.WriteFile(viper.GetString("key"), kb, 0600); err != nil {
			logger.Fatal(err)
		}
	} else {
		kb, err := ioutil.ReadFile(viper.GetString("key"))
		if err != nil {
			logger.Fatal(err)
		}

		key, err = crypto.UnmarshalPEM(kb)
		if err != nil {
			logger.Fatal(err)
		}
	}

	p, err := client.NewProxyClient(viper.GetString("proxy_endpoint"))
	if err != nil {
		logger.Fatal(err)
	} else {
		hostname, err := p.Register(key)
		if err != nil {
			logger.Fatal(err)
		}

		logger.Infof("Got hostname: %s", hostname)

		p.SetHandler(&httpClient{})

		if viper.GetString("state") != "" {
			s := state{
				Hostname: hostname,
			}

			sb, err := json.Marshal(s)
			if err != nil {
				logger.Fatal(err)
			}

			if err := ioutil.WriteFile(viper.GetString("state"), sb, 0600); err != nil {
				logger.Fatalf("Failed to write state file: %s", err)
			}
		}
	}

	p.Wait()
}

type state struct {
	Hostname string `json:"hostname"`
}

type httpClient struct{}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (h *httpClient) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger.Debugf("Request for %s%s", r.Host, r.URL.Path)

	// This is a websocket upgrade request, so let's setup a websocket client towards that same path on HomeAssistant and proxy the messages
	if strings.ToLower(r.Header.Get("Connection")) == "upgrade" && strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {

		respHeaders := make(http.Header)
		respHeaders.Add("Access-Control-Allow-Origin", r.Header.Get("Origin"))
		conn, err := upgrader.Upgrade(w, r, respHeaders)
		if err != nil {
			http.Error(w, errors.Wrap(err, "failed to upgrade to websocket").Error(), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		// Build local address
		host := strings.Replace(strings.Replace(viper.GetString("local"), "https://", "", 1), "http://", "", 1)
		schema := "ws"
		if strings.HasPrefix(viper.GetString("local"), "https://") {
			schema = "wss"
		}
		u := url.URL{Scheme: schema, Host: host, Path: r.URL.Path}

		// Remove headers that we should not forward
		headers := http.Header{}
		for k, v := range r.Header {
			switch strings.ToUpper(k) {
			case "CONNECTION":
			case "UPGRADE":
			case "SEC-WEBSOCKET-KEY":
			case "SEC-WEBSOCKET-VERSION":
			case "SEC-WEBSOCKET-EXTENSIONS":
			default:
				headers.Set(k, v[0])
			}
		}

		// Dial the local websocket
		clientConn, _, err := websocket.DefaultDialer.Dial(u.String(), headers)
		if err != nil {
			logger.Error(err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer clientConn.Close()

		// Read upstream messages and write to local websocket
		done := make(chan struct{})
		defer close(done)
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					typ, body, err := conn.ReadMessage()
					if err != nil {
						logger.Error(err)
						return
					}

					if err := clientConn.WriteMessage(typ, body); err != nil {
						logger.Error(err)
						return
					}
				}
				time.Sleep(time.Millisecond * 10)
			}
		}()

		// Read messages from local websocket and forward to upstream
		for {
			typ, body, err := clientConn.ReadMessage()
			if err != nil {
				logger.Error(err)
				return
			}

			if err := conn.WriteMessage(typ, body); err != nil {
				logger.Error(err)
				return
			}
			time.Sleep(time.Millisecond * 10)
		}
	} else {
		req, err := http.NewRequest(r.Method, fmt.Sprintf("%s%s", viper.GetString("local"), r.URL.Path), r.Body)
		if err != nil {
			logger.Error(err)
			return
		}

		for k, v := range r.Header {
			switch strings.ToUpper(k) {
			case "DNT":
				continue
			case "UPGRADE-INSECURE-REQUESTS":
				continue
			default:
				req.Header.Set(k, v[0])
			}
		}
		if viper.GetString("local_url") != "" {
			req.Header.Set("Host", viper.GetString("local_url"))
		} else {
			req.Header.Set("Host", r.Host)
		}

		client := &http.Client{
			Timeout: time.Second * 15,
		}

		res, err := client.Do(req)
		if err != nil {
			logger.Error(err)
			return
		}

		for k, v := range res.Header {
			w.Header().Set(k, v[0])
		}

		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logger.Error(err)
			return
		}

		w.Write(body)

		w.WriteHeader(res.StatusCode)
	}
}
