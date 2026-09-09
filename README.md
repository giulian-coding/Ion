# Ion

Ion is a repository for my private Lab Space and serves as a Central Personal
Assistance System. It is a personal engineering environment for experimenting
with services, infrastructure, automation, and assistant capabilities.

## Requirements

- Go `1.27.1`
- Docker with Docker Compose support

## Getting Started

1. Start the infrastructure services:

	```bash
	docker compose -f infrastructure/docker-compose.yaml up -d
	```

2. Start the worker manually:

	```bash
	go run ./cmd/worker
	```

3. In a second terminal, publish the sample jobs manually:

	```bash
	go run ./cmd/producer
	```

The worker connects to NATS at `nats://localhost:4222`, creates the `WORKQUEUE`
JetStream stream if necessary, and processes jobs from `jobs.backend`.

To stop the infrastructure:

```bash
docker compose -f infrastructure/docker-compose.yaml down
```

## Project Structure

```text
.
├── cmd/
│   ├── producer/       # Sample job publisher
│   └── worker/         # Worker and Queue environment initialization
├── infrastructure/    # Docker Compose and NATS configuration
├── internal/
│   └── job/            # Shared internal job contract
|   └── worker/         # Job Provessing service
├── go.mod
└── README.md
```

## Current Status

The services must currently be started manually with `go run`. Automated
application startup, service orchestration, and additional assistant modules
are planned for future iterations.

## TODOs
- [ ] Implement actual functionality based on Job Types
- [ ] Implement Golang Testing Cases