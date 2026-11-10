package main

import (
	hellopb "HelloProjectWithBalance/api/protobuf"
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver/dns"
)

func dial(addr string, roundRobin bool) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if roundRobin {
		opts = append(opts, grpc.WithDefaultServiceConfig(
			`{"loadBalancingConfig": [{"round_robin":{}}]}`))
	}

	return grpc.NewClient(addr, opts...)
}

func main() {
	dns.SetMinResolutionInterval(time.Second)
	addr := flag.String("addr", "hello:45001", "server address")
	n := flag.Int("n", 300, "number of calls")
	mode := flag.String("mode", "single", "single | per-call | rr")
	interval := flag.Duration("interval", 0, "pause between calls")
	flag.Parse()

	counts := make(map[string]int)

	call := func(conn *grpc.ClientConn) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		resp, err := hellopb.NewHelloServiceClient(conn).Hello(ctx, &hellopb.HelloRequest{}, grpc.WaitForReady(true))
		if err != nil {
			slog.Info("hello call failed", "error", err)
			return "", err
		}
		slog.Info("hello called", "pod", resp.GetPod())

		return resp.GetPod(), nil
	}

	switch *mode {
	case "single", "rr":
		conn, err := dial(*addr, *mode == "rr")
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()

		for range *n {
			pod, err := call(conn)
			if err != nil {
				counts["error"]++
			} else {
				counts[pod]++
			}
			time.Sleep(*interval)
		}
	case "per-call":
		for range *n {
			conn, err := dial(*addr, false)
			if err != nil {
				log.Fatal(err)
			}

			pod, err := call(conn)
			if err != nil {
				conn.Close()
				counts["error"]++
				continue
			}
			counts[pod]++
			conn.Close()
			time.Sleep(*interval)
		}
	default:
		log.Fatalf("unknown mode %q", *mode)
	}

	fmt.Println(counts)
}
