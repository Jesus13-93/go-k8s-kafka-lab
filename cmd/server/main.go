package main

import (
	"HelloProjectWithBalance/cmd/server/hello"
	"HelloProjectWithBalance/cmd/server/metrics"
	"context"
	"flag"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)
import protobuf "HelloProjectWithBalance/api/protobuf"

func main() {
	const op = "server"

	maxAge := flag.Duration("max-conn-age", 0, "max connection age, 0 - unlimited")
	grpcAddress := flag.String("grpc.listen-address", ":45001", "gRPC listen address host:port")
	work := flag.Duration("work", 0, "busy CPU time per request, e.g. 5ms")

	flag.Parse()

	var opts []grpc.ServerOption
	if *maxAge >= 0 {
		opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      *maxAge,
			MaxConnectionAgeGrace: 5 * time.Second,
		}))
	} else {
		log.Fatalf("max-conn-age must be >= 0, got %v", *maxAge)
	}
	opts = append(opts, grpc.UnaryInterceptor(metrics.MetricsInterceptor))
	srv := grpc.NewServer(opts...)

	helloHandler := hello.NewHelloHandler(*work)
	protobuf.RegisterHelloServiceServer(srv, helloHandler)

	appCtx, appCancelFn := signal.NotifyContext(
		context.Background(),
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	defer appCancelFn()

	lc := net.ListenConfig{}

	grpcListener, err := lc.Listen(appCtx, "tcp", *grpcAddress)
	if err != nil {
		log.Panicf("%s: error creating gRPC listener: %v", op, err)
	}

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", mux); err != nil {
			log.Printf("metrics server: %v", err)
		}
	}()

	go func() {
		slog.Info("starting grpc server", "address", *grpcAddress)

		if err = srv.Serve(grpcListener); err != nil {
			log.Panicf("%s: error starting grpc server: %v", op, err)
		}
	}()

	<-appCtx.Done()
	slog.Info("received os signal")

	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("Server gracefully stopped")
	case <-time.After(5 * time.Second):
		slog.Warn("graceful stop timed out, forcing")
		srv.Stop()
	}

	slog.Info("Server stopped")

}
