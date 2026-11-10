package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const schema = `
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS charges (
    id              bigserial PRIMARY KEY,
    order_id        text   NOT NULL,
    status          text   NOT NULL,
    kafka_partition int    NOT NULL,
    kafka_offset    bigint NOT NULL
);`

func main() {
	brokers := flag.String("brokers", "my-cluster-kafka-bootstrap.kafka:9092", "адрес Kafka")
	topic := flag.String("topic", "orders", "топик")
	group := flag.String("group", "go-billing", "consumer group")
	crashAfter := flag.Int("crash-after", 0, "упасть после N сообщений, не закоммитив (0 = не падать)")
	dsn := flag.String("dsn", "postgres://app:app@postgres:5432/app?sslmode=disable", "Postgres DSN")
	idempotent := flag.Bool("idempotent", false, "защита от повторной обработки")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Fatal(err)
	}

	_, err = pool.Exec(ctx, schema)
	if err != nil {
		log.Fatal(err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(*brokers),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumerGroup(*group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	processed := 0
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(s string, i int32, err error) {
			log.Printf("fetch error %s/%d: %v", s, i, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			if err := process(ctx, pool, record, *idempotent); err != nil {
				// пока просто падаем: без коммита сообщение придёт снова.
				// правильная обработка ошибок — это тема DLQ, сделаем позже
				log.Fatalf("process p=%d off=%d: %v", record.Partition, record.Offset, err)
			}
			processed++

			if *crashAfter > 0 && processed == *crashAfter {
				log.Printf("💥 падаю, не закоммитив")
				os.Exit(1)
			}
		})

		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit: %v", err)
		}
	}
}

func process(ctx context.Context, pool *pgxpool.Pool, r *kgo.Record, idempotent bool) error {
	orderID, status := string(r.Key), string(r.Value)
	eventID := orderID + ":" + status

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if idempotent {
		tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			log.Printf("p=%d off=%d %s — дубль, пропускаю", r.Partition, r.Offset, eventID)
			return nil
		}
	}

	if _, err = tx.Exec(ctx, `INSERT INTO charges (order_id, status, kafka_partition, kafka_offset) VALUES ($1, $2, $3, $4)`, orderID, status, r.Partition, r.Offset); err != nil {
		return err
	}

	log.Printf("p=%d off=%d %s — списано", r.Partition, r.Offset, eventID)
	return tx.Commit(ctx)
}
