package transport

//go:generate protoc -I$GOPATH/src -I./pb -I. --go_out=paths=source_relative:./pb transport.proto
//go:generate protoc -I$GOPATH/src -I./pb -I. --go-grpc_out=paths=source_relative:./pb transport.proto
