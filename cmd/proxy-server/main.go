package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Brickchain/go-logger.v1"
	"github.com/Brickchain/go-proxy.v1/pkg/server/clients"
	"github.com/Brickchain/go-proxy.v1/pkg/version"

	"path"

	cache "github.com/Brickchain/go-cache.v1"
	httphandler "github.com/Brickchain/go-httphandler.v2"
	"github.com/Brickchain/go-proxy.v1/pkg/server/api"
	pubsub "github.com/Brickchain/go-pubsub.v1"
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
	"github.com/tylerb/graceful"

	redis "github.com/go-redis/redis"
	gorillaHandlers "github.com/gorilla/handlers"
	"github.com/ulule/limiter"
	"github.com/ulule/limiter/drivers/store/memory"
	limiterredis "github.com/ulule/limiter/drivers/store/redis"
)

func main() {
	_ = godotenv.Load(".env")
	viper.AutomaticEnv()
	viper.SetDefault("log_formatter", "text")
	viper.SetDefault("log_level", "debug")
	viper.SetDefault("addr", ":6519")
	viper.SetDefault("base", "http://localhost:6519")
	viper.SetDefault("db", "gorm")
	viper.SetDefault("cache", "inmem")
	viper.SetDefault("redis", "localhost:6379")
	viper.SetDefault("pubsub", "inmem")
	viper.SetDefault("domain", "r.localhost:6519")

	logger.SetOutput(os.Stdout)
	logger.SetFormatter(viper.GetString("log_formatter"))
	logger.SetLevel(viper.GetString("log_level"))
	logger.AddContext("service", path.Base(os.Args[0]))
	logger.AddContext("version", version.Version)

	addr := viper.GetString("addr")
	server := &graceful.Server{
		Timeout: time.Duration(15) * time.Second,
		Server: &http.Server{
			Addr:    addr,
			Handler: loadHandler(),
		},
	}

	logger.Info("server starting at " + addr)
	if err := server.ListenAndServe(); err != nil {
		logger.Fatal(err)
	}
}

func loadHandler() http.Handler {

	wrappers := httphandler.NewWrapper(false)

	pubsub := loadPubSub()

	cache := loadCache()

	clients := clients.NewClientService(cache)

	r := httphandler.NewRouter()
	r.GET("/", wrappers.Wrap(api.Version))

	store, err := loadLimiterStore()
	if err != nil {
		logger.Fatal(err)
	}
	limiter := limiter.New(store, limiter.Rate{
		Period: 5 * time.Minute,
		Limit:  500,
	})
	requestController := api.NewRequestController(viper.GetString("domain"), clients, pubsub, limiter)
	r.GET("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))
	r.POST("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))
	r.PUT("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))
	r.DELETE("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))
	r.OPTIONS("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))

	subscribeController := api.NewSubscribeController(viper.GetString("domain"), clients, pubsub)
	r.GET("/proxy/subscribe", wrappers.Wrap(subscribeController.SubscribeHandler))

	handler := gorillaHandlers.CORS(gorillaHandlers.IgnoreOptions())(r)

	if viper.GetString("domain") != "" {
		dr := httphandler.NewRouter()
		dr.GET("/*filepath", wrappers.Wrap(requestController.Handle))
		dr.POST("/*filepath", wrappers.Wrap(requestController.Handle))
		dr.PUT("/*filepath", wrappers.Wrap(requestController.Handle))
		dr.DELETE("/*filepath", wrappers.Wrap(requestController.Handle))
		dr.OPTIONS("/*filepath", wrappers.Wrap(requestController.Handle))

		domainHandler := gorillaHandlers.CORS(gorillaHandlers.IgnoreOptions())(dr)

		return &domainRouter{viper.GetString("domain"), handler, domainHandler}
	}

	return handler
}

type domainRouter struct {
	domain        string
	handler       http.Handler
	domainHandler http.Handler
}

func (d *domainRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	host := req.Host
	if strings.HasSuffix(host, "."+d.domain) {
		d.domainHandler.ServeHTTP(w, req)
	} else {
		d.handler.ServeHTTP(w, req)
	}
}

func loadCache() cache.Cache {
	switch viper.GetString("cache") {
	case "file":
		c, err := cache.NewFile(viper.GetString("cache_dir"), time.Second*1)
		if err != nil {
			logger.Fatal(err)
		}
		return c
	case "inmem":
		return cache.NewInmem()
	case "redis":
		return cache.NewRedisCache(viper.GetString("redis"))
	default:
		logger.Fatal("Cache type not supported: " + viper.GetString("cache"))
	}

	return nil
}

func loadPubSub() pubsub.PubSubInterface {
	var p pubsub.PubSubInterface
	var err error
	switch viper.GetString("pubsub") {
	case "google":
		p, err = pubsub.NewGCloudPubSub(viper.GetString("pubsub_project_id"), viper.GetString("pubsub_credentials"))
	case "redis":
		p, err = pubsub.NewRedisPubSub(viper.GetString("redis"))
	case "inmem":
		p = pubsub.NewInmemPubSub()
	default:
		logger.Fatal("PubSub type not supported")
	}
	if err != nil {
		logger.Fatal(err)
	}

	return p
}

func loadLimiterStore() (limiter.Store, error) {
	switch viper.GetString("cache") {
	case "inmem":
		return memory.NewStore(), nil
	case "redis":
		option, err := redis.ParseURL(fmt.Sprintf("redis://%s/0", viper.GetString("redis")))
		if err != nil {
			return nil, err
		}
		client := redis.NewClient(option)
		return limiterredis.NewStoreWithOptions(client, limiter.StoreOptions{
			Prefix:   "push",
			MaxRetry: 4,
		})
	default:
		return nil, errors.New("Unknown type")
	}
}
