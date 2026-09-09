package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"Ion/internal/job"
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
