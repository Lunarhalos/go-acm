package main

//go:generate protoc -I$GOPATH/src -I./api/acmserverpb --go_out=paths=source_relative:./api/acmserverpb acmserver.proto
//go:generate protoc -I$GOPATH/src -I./api/acmserverpb --go-grpc_out=paths=source_relative:./api/acmserverpb acmserver.proto
