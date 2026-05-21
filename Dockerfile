FROM golang:1.23-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/worker ./cmd/worker

FROM alpine:3.19 AS runtime
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /bin/server /app/server
COPY --from=builder /bin/worker /app/worker
COPY web /app/web
EXPOSE 8080
CMD ["/app/server"]
