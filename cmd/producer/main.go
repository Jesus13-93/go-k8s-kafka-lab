package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "my-cluster-kafka-bootstrap.kafka:9092", "адрес Kafka")
	topic := flag.String("topic", "orders", "топик")
	orders := flag.Int("orders", 5, "сколько заказов")
	flag.Parse()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(*brokers),
		kgo.DefaultProduceTopic(*topic))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	for _, step := range []string{"создан", "оплачен", "отправлен"} {
		for i := 1; i <= *orders; i++ {
			key := fmt.Sprintf("order-%d", i)
			rec := &kgo.Record{Key: []byte(key), Value: []byte(step)}

			if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
				log.Printf("produce %v", err)
			}
			log.Printf("sent %s:%s -> p=%d off=%d", key, step, rec.Partition, rec.Offset)
		}
	}
}
