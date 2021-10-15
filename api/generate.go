package api

//go:generate protoc -I$GOPATH/src -I./acmserverpb -I. --go_out=paths=source_relative:./acmserverpb rpc.proto
//go:generate protoc -I$GOPATH/src -I./acmserverpb -I. --go-grpc_out=paths=source_relative:./acmserverpb rpc.proto
