GOPATH:=$(shell go env GOPATH)
VERSION=$(shell git describe --tags --always)
API_PROTO_FILES=$(shell find api -name *.proto)

.PHONY: proto
# generate internal proto
proto:
	protoc --proto_path=. \
 		--go_out=paths=source_relative:. \
 		--go-grpc_out=paths=source_relative:. \
		$(API_PROTO_FILES)

.PHONY: build
# build
build:
	mkdir -p bin/ && go build -ldflags "-X main.Version=$(VERSION)" -o ./bin/ ./...

.PHONY: test
# test
test:
	go test -v -cover ./...

.PHONY: all
# generate all
all:
	make proto;
	make build;
	make test;
