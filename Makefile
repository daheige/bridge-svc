.PHONY: all build proto test clean docker

BINARY_NAME=bridge-svc
DOCKER_IMAGE=your-registry/bridge-svc
VERSION=$(shell git describe --tags --always --dirty)

all: proto build

# 生成 protobuf 代码
proto:
	@echo "Generating protobuf code..."
	@protoc -I api/v1 \
           --go_out=api/v1 --go_opt=paths=source_relative \
           --go-grpc_out=api/v1 --go-grpc_opt=paths=source_relative \
           api/v1/bridge.proto
	@echo "gen code success"

# 构建二进制
build:
	@echo "Building $(BINARY_NAME)..."
	@go build -ldflags "-X main.version=$(VERSION)" -o bin/$(BINARY_NAME) ./cmd/bridge

# 运行测试
test:
	@go test -v -race ./...

# 清理
clean:
	@rm -rf bin/
	@rm -f api/v1/*.pb.go

# 构建 Docker 镜像（基于 Go 1.24）
docker:
	@docker build -t $(DOCKER_IMAGE):$(VERSION) -t $(DOCKER_IMAGE):latest .

# 本地运行
run: build
	@./bin/$(BINARY_NAME)

# 格式化代码
fmt:
	@gofmt -w -s .
	@goimports -w .

# 静态检查
lint:
	@golangci-lint run ./...

# 依赖更新（升级到最新版本）
deps:
	@go get -u ./...
	@go mod tidy
	@go mod verify
