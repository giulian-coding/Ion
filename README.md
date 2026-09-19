# Ion

Ion is a repository for my private Lab Space and serves as a Central Personal
Assistance System. It is a personal engineering environment for experimenting
with services, infrastructure, automation, and assistant capabilities.

## Requirements

- Go `1.27.1`
- Docker with Docker Compose support

## Getting Started

1. Set the folllowing environment Variables for your containers:
   SYS_PASSWORD, APP_PASSWORD, WEB_PASSWORD

2. Start the infrastructure services:

    ```bash
    docker compose -f infrastructure/docker-compose.yaml up -d
    ```

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