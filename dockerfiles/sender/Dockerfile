FROM golang:1.25-alpine AS builder

RUN apk add --no-cache build-base

WORKDIR /app

COPY . .
RUN go mod download

#https://github.com/confluentinc/confluent-kafka-go#librdkafka
RUN go build -tags musl -o /bin/sender ./cmd/sender

FROM alpine:3.21

COPY --from=builder /bin/sender /bin/sender

ENTRYPOINT ["/bin/sender"]
