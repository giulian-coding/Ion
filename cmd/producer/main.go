package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"Ion/internal/job"

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

	ctx := context.Background()
	streamConfig := jetstream.StreamConfig{
		Name:      "WORKQUEUE",
		Subjects:  []string{"jobs.backend"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
	}

	if _, err = js.CreateOrUpdateStream(ctx, streamConfig); err != nil {
		log.Fatalf("error creating stream: %v", err)
	}
	log.Println("stream verified")

	for index := 1; index <= 5; index++ {
		queuedJob := job.Job{
			ID: fmt.Sprintf("test-job-%d", index),
			Payload: map[string]any{
				"task": "simulate_work",
				"time": 3,
			},
		}

		jobBytes, err := json.Marshal(queuedJob)
		if err != nil {
			log.Fatalf("error encoding job %d: %v", index, err)
		}

		if _, err = js.Publish(ctx, "jobs.backend", jobBytes); err != nil {
			log.Fatalf("error publishing job %d: %v", index, err)
		}

		log.Printf("job %s published", queuedJob.ID)
	}
}
