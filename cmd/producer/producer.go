// producer.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type TestJob struct {
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
		log.Fatalf("Fehler beim Erstellen des Streams: %v", err)
	}
	fmt.Println("Stream erfolgreich verifiziert.")

	// Jobs pushen
	for i := 1; i <= 5; i++ {
		job := TestJob{
			ID: fmt.Sprintf("test-job-%d", i),
			Payload: map[string]any{
				"task": "simulate_work",
				"time": 3,
			},
		}

		jobBytes, _ := json.Marshal(job)

		_, err = js.Publish(ctx, "jobs.backend", jobBytes)
		if err != nil {
			log.Fatalf("Fehler beim Senden von Job %d: %v", i, err)
		}

		fmt.Printf("Job %s in die Queue geschickt.\n", job.ID)
	}
}
