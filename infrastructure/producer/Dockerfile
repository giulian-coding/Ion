FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY ../../ .
RUN CGO_ENABLED=0 go build -o /worker ./cmd/producer

FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 app
COPY --from=build /worker /worker
USER app
ENTRYPOINT ["/worker"]