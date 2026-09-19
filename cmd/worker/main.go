package main

import (
	"Ion/internal/worker"
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		log.Fatal("NATS_URL not set")
	}

	pass := os.Getenv("APP_PASSWORD")
	if pass == "" {
		log.Fatal("APP_PASSWORD not set")
	}

	nc, err := nats.Connect(url,
		nats.UserInfo("backend", pass),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		log.Fatalf("nats connection failed: %v", err)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	natsCtx := context.Background()
	streamConfig := jetstream.StreamConfig{
		Name:      "WORKQUEUE",
		Subjects:  []string{"jobs.backend"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
	}

	if _, err = js.CreateOrUpdateStream(natsCtx, streamConfig); err != nil {
		log.Fatal(err)
	}

	consumer, err := js.CreateOrUpdateConsumer(natsCtx, "WORKQUEUE", jetstream.ConsumerConfig{
		Durable:   "job-processors",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelGlobal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelGlobal()

	jobChan := make(chan jetstream.Msg, 50)
	var workers sync.WaitGroup

	worker.Start(ctx, jobChan, &workers)

	consumeContext, err := consumer.Consume(func(msg jetstream.Msg) {
		jobChan <- msg
	})
	if err != nil {
		log.Fatal(err)
	}

	// graceful shutdown section
	<-ctx.Done()
	log.Println("graceful shutdown initialized")

	consumeContext.Stop()
	close(jobChan)

	workers.Wait()

	log.Println("workers successfully shutdown")
}
