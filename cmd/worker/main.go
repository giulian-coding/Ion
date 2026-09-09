package main

import (
	"Ion/internal/worker"
	"context"
	"log"
	"os/signal"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

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
