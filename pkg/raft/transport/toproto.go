package transport

import (
	"github.com/Lunarhalos/go-acm/pkg/raft/transport/raftpb"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func encodeAppendEntriesRequest(s *raft.AppendEntriesRequest) *raftpb.AppendEntriesRequest {
	return &raftpb.AppendEntriesRequest{
		RpcHeader:         encodeRPCHeader(s.RPCHeader),
		Term:              s.Term,
		Leader:            s.Leader,
		PrevLogEntry:      s.PrevLogEntry,
		PrevLogTerm:       s.PrevLogTerm,
		Entries:           encodeLogs(s.Entries),
		LeaderCommitIndex: s.LeaderCommitIndex,
	}
}

func encodeRPCHeader(s raft.RPCHeader) *raftpb.RPCHeader {
	return &raftpb.RPCHeader{
		ProtocolVersion: int64(s.ProtocolVersion),
	}
}

func encodeLogs(s []*raft.Log) []*raftpb.Log {
	ret := make([]*raftpb.Log, len(s))
	for i, l := range s {
		ret[i] = encodeLog(l)
	}
	return ret
}

func encodeLog(s *raft.Log) *raftpb.Log {
	return &raftpb.Log{
		Index:      s.Index,
		Term:       s.Term,
		Type:       encodeLogType(s.Type),
		Data:       s.Data,
		Extensions: s.Extensions,
		AppendedAt: timestamppb.New(s.AppendedAt),
	}
}

func encodeLogType(s raft.LogType) raftpb.LogType {
	switch s {
	case raft.LogCommand:
		return raftpb.LogType_LogCommand
	case raft.LogNoop:
		return raftpb.LogType_LogNoop
	case raft.LogAddPeerDeprecated:
		return raftpb.LogType_LogAddPeerDeprecated
	case raft.LogRemovePeerDeprecated:
		return raftpb.LogType_LogRemovePeerDeprecated
	case raft.LogBarrier:
		return raftpb.LogType_LogBarrier
	case raft.LogConfiguration:
		return raftpb.LogType_LogConfiguration
	default:
		panic("invalid LogType")
	}
}

func encodeAppendEntriesResponse(s *raft.AppendEntriesResponse) *raftpb.AppendEntriesResponse {
	return &raftpb.AppendEntriesResponse{
		RpcHeader:      encodeRPCHeader(s.RPCHeader),
		Term:           s.Term,
		LastLog:        s.LastLog,
		Success:        s.Success,
		NoRetryBackoff: s.NoRetryBackoff,
	}
}

func encodeRequestVoteRequest(s *raft.RequestVoteRequest) *raftpb.RequestVoteRequest {
	return &raftpb.RequestVoteRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		Term:               s.Term,
		Candidate:          s.Candidate,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		LeadershipTransfer: s.LeadershipTransfer,
	}
}

func encodeRequestVoteResponse(s *raft.RequestVoteResponse) *raftpb.RequestVoteResponse {
	return &raftpb.RequestVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Peers:     s.Peers,
		Granted:   s.Granted,
	}
}

func encodeInstallSnapshotRequest(s *raft.InstallSnapshotRequest) *raftpb.InstallSnapshotRequest {
	return &raftpb.InstallSnapshotRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		SnapshotVersion:    int64(s.SnapshotVersion),
		Term:               s.Term,
		Leader:             s.Leader,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		Peers:              s.Peers,
		Configuration:      s.Configuration,
		ConfigurationIndex: s.ConfigurationIndex,
		Size:               s.Size,
	}
}

func encodeInstallSnapshotResponse(s *raft.InstallSnapshotResponse) *raftpb.InstallSnapshotResponse {
	return &raftpb.InstallSnapshotResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Success:   s.Success,
	}
}

func encodeTimeoutNowRequest(s *raft.TimeoutNowRequest) *raftpb.TimeoutNowRequest {
	return &raftpb.TimeoutNowRequest{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func encodeTimeoutNowResponse(s *raft.TimeoutNowResponse) *raftpb.TimeoutNowResponse {
	return &raftpb.TimeoutNowResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}
