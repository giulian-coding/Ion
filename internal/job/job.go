package job

// Job is the message exchanged through the NATS work queue.
type Job struct {
	ID      string         `json:"id"`
	Payload map[string]any `json:"payload"`
}
