package worker

import (
	"Ion/internal/job"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func Start(ctx context.Context, jobChan <-chan jetstream.Msg, workers *sync.WaitGroup) {
	numWorkers := runtime.NumCPU()
	for workerID := range numWorkers {
		workers.Add(1)
		go run(ctx, workerID, jobChan, workers)
	}

	log.Printf("Backend initialized. %d workers started.", numWorkers)
}

func run(ctx context.Context, workerID int, jobChan <-chan jetstream.Msg, workers *sync.WaitGroup) {
	defer workers.Done()

	for msg := range jobChan {
		var queuedJob job.Job
		if err := json.Unmarshal(msg.Data(), &queuedJob); err != nil {
			_ = msg.Term()
			continue
		}

		log.Printf("[Worker %d] start job: %s", workerID, queuedJob.ID)
		jobCtx, cancelJob := context.WithTimeout(ctx, 10*time.Second) // Job will be canceled if it runs for 10 Seconds
		err := handle(jobCtx, queuedJob)
		cancelJob()

		if err != nil {
			log.Printf("[Worker %d] job %s failed/canceled: %v", workerID, queuedJob.ID, err)
			_ = msg.Nak()
			continue
		}

		_ = msg.Ack()
		log.Printf("[Worker %d] job %s completed.", workerID, queuedJob.ID)
	}

	log.Printf("[Worker %d] channel closed. worker will shutdown.", workerID)
}

func handle(ctx context.Context, queuedJob job.Job) error {
	select {
	case <-time.After(3 * time.Second):
		log.Println(queuedJob.ID)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("work canceled: %w", ctx.Err())
	}
}
