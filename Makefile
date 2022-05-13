GOPATH:=$(shell go env GOPATH)
VERSION=$(shell git describe --tags --always)
API_PROTO_FILES=$(shell find api -name *.proto)
PWD:=$(shell pwd)


.PHONY: proto
# generate proto
proto:
	protoc --proto_path=. \
		--proto_path=$(PWD)/third_party \
 		--go_out=paths=source_relative:. \
 		--go-grpc_out=paths=source_relative:. \
 		--go-http_out=paths=source_relative:. \
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
