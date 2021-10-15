package transport

//go:generate protoc -I$GOPATH/src -I./raftpb -I. --go_out=paths=source_relative:./raftpb rpc.proto
//go:generate protoc -I$GOPATH/src -I./raftpb -I. --go-grpc_out=paths=source_relative:./raftpb rpc.proto
