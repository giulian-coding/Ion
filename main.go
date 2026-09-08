package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Job struct {
	ID      string         `json:"id"`
	Payload map[string]any `json:"payload"`
}

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

	ctx := context.Background()

	streamConfig := jetstream.StreamConfig{
		Name:      "WORKQUEUE",
		Subjects:  []string{"jobs.backend"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
	}

	_, err = js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		log.Fatal(err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, "WORKQUEUE", jetstream.ConsumerConfig{
		Durable:   "job-processors",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelGlobal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelGlobal()

	jobChan := make(chan jetstream.Msg, 50)
	var wg sync.WaitGroup

	numWorkers := runtime.NumCPU()
	for id := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for msg := range jobChan {
				var job Job
				if err := json.Unmarshal(msg.Data(), &job); err != nil {
					msg.Term()
					continue
				}

				log.Printf("[Worker %d] start job: %s\n", workerID, job.ID)

				jobCtx, cancelJob := context.WithTimeout(ctx, 10*time.Second)
				err := handle(jobCtx, job)

				cancelJob()

				if err != nil {
					log.Printf("[Worker %d] job %s failed/canceled: %v\n", workerID, job.ID, err)
					msg.Nak()
					continue
				}

				msg.Ack()
				log.Printf("[Worker %d] Job %s completed.\n", workerID, job.ID)
			}

			log.Printf("[Worker %d] channel closed. worker will shutdown.\n", workerID)
		}(id)
	}

	consumeCtx, err := cons.Consume(func(msg jetstream.Msg) {
		jobChan <- msg
	})

	log.Printf("Backend initialized. %d workers started.", numWorkers)

	<-ctx.Done()
	log.Println("graceful shutdown initialized")

	consumeCtx.Stop()
	close(jobChan)

	shutdownWg := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownWg)
	}()

	select {
	case <-shutdownWg:
		log.Println("all workers gracefully shut down.")
	case <-time.After(5 * time.Second):
		log.Println("timeout waiting for workers. will force shutdown.")
	}
}

func handle(ctx context.Context, job Job) error {

	select {
	case <-time.After(3 * time.Second):
		log.Println(job.ID)
		return nil

	case <-ctx.Done():
		return fmt.Errorf("work canceled: %w", ctx.Err())
	}
}
