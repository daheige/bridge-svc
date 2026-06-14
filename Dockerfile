# 构建阶段（Go 1.24 Alpine）
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -ldflags "-w -s" -o bridge-svc ./cmd/bridge

# 运行阶段
FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /app/

COPY --from=builder /app/bridge-svc /app/bridge-svc
COPY config/bridge.yaml ./config/

EXPOSE 50051 9090

ENTRYPOINT ["/app/bridge-svc"]