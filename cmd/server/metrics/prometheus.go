package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var reqDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "hello_request_duration_seconds",
	Help:    "Длительность обработки gRPC-запросов",
	Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // корзины от 1 мс до ~2 с
}, []string{"method", "code"})

func init() {
	prometheus.MustRegister(reqDuration)
}

func MetricsInterceptor(
	ctx context.Context, req any,
	info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req) // вызываем настоящий хендлер
	reqDuration.
		WithLabelValues(info.FullMethod, status.Code(err).String()).
		Observe(time.Since(start).Seconds())
	return resp, err
}
