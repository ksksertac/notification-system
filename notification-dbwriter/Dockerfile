FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY shared/ ./shared/
COPY notification-dbwriter/ ./notification-dbwriter/

WORKDIR /app/notification-dbwriter
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /service .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /service .
COPY --from=builder /app/notification-dbwriter/migrations ./migrations

ENTRYPOINT ["./service"]
