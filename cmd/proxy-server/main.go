package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	logger "github.com/Brickchain/go-logger.v1"
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
	viper.SetDefault("domain", "")

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

	logger.Infof("server with version %s starting at %s", version.Version, addr)
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

	subscribeController := api.NewSubscribeController(viper.GetString("domain"), clients, pubsub)
	r.GET("/proxy/subscribe", wrappers.Wrap(subscribeController.SubscribeHandler))

	apiHandler := httphandler.LoadMiddlewares(r, version.Version)

	store, err := loadLimiterStore()
	if err != nil {
		logger.Fatal(err)
	}
	limiter := limiter.New(store, limiter.Rate{
		Period: 5 * time.Minute,
		Limit:  500,
	})
	requestHandler := httphandler.NewRouter()
	requestController := api.NewRequestController(viper.GetString("domain"), clients, pubsub, limiter)
	requestHandler.GET("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))
	requestHandler.POST("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))
	requestHandler.PUT("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))
	requestHandler.DELETE("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))
	requestHandler.OPTIONS("/proxy/request/:clientID/*filepath", wrappers.Wrap(requestController.Handle))

	domainHandler := httphandler.NewRouter()
	if viper.GetString("domain") != "" {
		domainHandler.GET("/*filepath", wrappers.Wrap(requestController.Handle))
		domainHandler.POST("/*filepath", wrappers.Wrap(requestController.Handle))
		domainHandler.PUT("/*filepath", wrappers.Wrap(requestController.Handle))
		domainHandler.DELETE("/*filepath", wrappers.Wrap(requestController.Handle))
		domainHandler.OPTIONS("/*filepath", wrappers.Wrap(requestController.Handle))
	}

	return &requestRouter{
		domain:         viper.GetString("domain"),
		apiHandler:     apiHandler,
		requestHandler: requestHandler,
		domainHandler:  domainHandler,
	}
}

type requestRouter struct {
	domain         string
	apiHandler     http.Handler
	requestHandler http.Handler
	domainHandler  http.Handler
}

func (d *requestRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	host := req.Host
	if d.domain != "" && strings.HasSuffix(host, "."+d.domain) {
		d.domainHandler.ServeHTTP(w, req)
	} else {
		if strings.HasPrefix(req.URL.Path, "/proxy/request/") {
			d.requestHandler.ServeHTTP(w, req)
		} else {
			d.apiHandler.ServeHTTP(w, req)
		}
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
