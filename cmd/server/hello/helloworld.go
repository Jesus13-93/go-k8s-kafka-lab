package hello

import (
	protobufs "HelloProjectWithBalance/api/protobuf"
	"context"
	"os"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	work time.Duration
	protobufs.UnsafeHelloServiceServer
}

func NewHelloHandler(work time.Duration) *Handler {
	return &Handler{work: work}
}

func (h *Handler) Hello(ctx context.Context, _ *protobufs.HelloRequest) (*protobufs.HelloResponse, error) {
	if h.work > 0 {
		deadline := time.Now().Add(h.work)
		x := 0
		for time.Now().Before(deadline) {
			x++ // крутим процессор
		}
		_ = x
	}
	pod, err := os.Hostname()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "hostname: %v", err)
	}
	return &protobufs.HelloResponse{Pod: pod}, nil

}
