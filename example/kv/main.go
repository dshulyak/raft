package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dshulyak/raft"
	"github.com/dshulyak/raft/transport/grpcstream"
	"github.com/dshulyak/raft/types"
	"github.com/gorilla/mux"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

var (
	name     = flag.String("name", "", "unique name of this node")
	dataDir  = flag.StringP("data", "d", "", "directory for the persistent data")
	conf     = flag.StringP("conf", "c", "", "cluster configuration")
	logLevel = flag.StringP("log-level", "v", "debug", "log level")
	listen   = flag.StringP("listen", "l", "0.0.0.0:8001", "listener address for kv application")
)

func makeLogger() *zap.Logger {
	var level zapcore.Level
	if err := level.Set(*logLevel); err != nil {
		panic(err.Error())
	}
	logger, err := zap.NewDevelopment(zap.IncreaseLevel(level))
	if err != nil {
		panic(err.Error())
	}
	return logger
}

func main() {
	flag.Parse()

	logger := makeLogger().Sugar()

	if len(*name) == 0 {
		logger.Fatal("name is required")
	}
	if len(*conf) == 0 {
		logger.Fatal("configuration is required")
	}
	if len(*dataDir) == 0 {
		logger.Fatal("directory path is empty")
	}
	_, err := os.Stat(*dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Fatalf("data directory %s must be created\n", *dataDir)
		}
		logger.Fatalf("error %v", err)
	}

	var cluster types.Configuration
	f, err := os.Open(*conf)
	if err != nil {
		logger.Fatalf("can't open config at %v: %v\n", *conf, err)
	}
	if err := yaml.NewDecoder(f).Decode(&cluster); err != nil {
		logger.Fatalf("can't decode config at %v into types.Configuration: %v\n", *conf, err)
	}

	var node *types.Node
	for i := range cluster.Nodes {
		if cluster.Nodes[i].Name == *name {
			node = &cluster.Nodes[i]
		}
	}
	if node == nil {
		logger.Fatalf("node with id %d is absent from configuration\n", *name)
	}

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)

	//
	// SETUP GRPC TRANSPORT
	//

	glisten, err := net.Listen("tcp", node.Address)
	if err != nil {
		logger.Fatalf("failed to listen on %v: %v\n", node.Address, err)
	}
	gsrv := grpc.NewServer()

	// transport register handler for the stream. grpc doesn't allow for the handlers
	// to be registered after server is started.
	tr := grpcstream.NewTransport(node.ID, gsrv)
	group.Go(func() error {
		return gsrv.Serve(glisten)
	})

	//
	// SETUP CONFIG FOR RAFT NODE
	//

	app := newKv(logger)

	conf := raft.DefaultConfig
	conf.ID = node.ID
	// this is temporary until membership changes are supported
	conf.Configuration = &cluster
	conf.App = app
	conf.Logger = logger.Desugar()
	conf.Transport = tr
	conf.TickInterval = time.Second

	if err := raft.BuildConfig(&conf,
		raft.WithStorageAt(*dataDir),
		raft.WithStateAt(*dataDir),
	); err != nil {
		logger.Fatalf("failed to build config %v\n", err)
	}

	rnode := raft.NewNode(&conf)
	group.Go(func() error {
		err := rnode.Wait()
		conf.Storage.Close()
		conf.State.Close()
		return err
	})

	//
	// SETUP HTTP SERVER FOR KV APP
	//

	router := mux.NewRouter()
	srv := server{
		app:  app,
		raft: rnode,
	}
	registerServer(&srv, router)
	hsrv := &http.Server{
		Addr:         *listen,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	group.Go(func() error {
		return hsrv.ListenAndServe()
	})

	group.Go(func() error {
		<-ctx.Done()
		rnode.Close()
		gsrv.GracefulStop()
		hsrv.Close()
		return nil
	})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)
	group.Go(func() error {
		select {
		case <-sig:
			logger.Info("received interrupt")
			cancel()
			return context.Canceled
		case <-ctx.Done():
			return nil
		}
	})

	if err := group.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("node was stopped")
		} else {
			logger.Fatalf("node failed: %v\n", err)
		}
	}

}
