FROM golang:1.26-alpine AS builder
WORKDIR /app
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/worker ./cmd/worker

FROM alpine:3.19 AS runtime
RUN apk --no-cache add ca-certificates tzdata
RUN addgroup -S gophprofile && adduser -S -G gophprofile -u 10001 gophprofile
WORKDIR /app
COPY --from=builder /bin/server /app/server
COPY --from=builder /bin/worker /app/worker
COPY web /app/web
RUN chown -R gophprofile:gophprofile /app
USER 10001:10001
EXPOSE 8080
CMD ["/app/server"]
