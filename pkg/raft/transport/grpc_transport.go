package transport

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/Lunarhalos/go-acm/pkg/raft/transport/raftpb"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type GRPCTransport interface {
	raft.Transport
	Register(s *grpc.Server)
}

type gRPCTransport struct {
	localAddress    raft.ServerAddress
	dialOptions     []grpc.DialOption
	consumeCh       chan raft.RPC
	heartbeatFn     func(raft.RPC)
	heartbeatFnLock sync.Mutex
	clientsLock     sync.Mutex
	clients         map[raft.ServerAddress]*client
}

type client struct {
	c    raftpb.RaftClient
	lock sync.Mutex
}

// NewGRPCTransport creates both components of raft-grpc-transport: a gRPC service and a Raft GRPCTransport.
func NewGRPCTransport(localAddress raft.ServerAddress, dialOptions ...grpc.DialOption) GRPCTransport {
	t := &gRPCTransport{
		localAddress:    localAddress,
		dialOptions:     dialOptions,
		consumeCh:       make(chan raft.RPC),
		heartbeatFn:     nil,
		heartbeatFnLock: sync.Mutex{},
		clientsLock:     sync.Mutex{},
		clients:         nil,
	}
	return t
}

// Consumer returns a channel that can be used to consume and respond to RPC requests.
func (t *gRPCTransport) Consumer() <-chan raft.RPC {
	return t.consumeCh
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (t *gRPCTransport) LocalAddr() raft.ServerAddress {
	return t.localAddress
}

func (t *gRPCTransport) getClient(target raft.ServerAddress) (raftpb.RaftClient, error) {
	t.clientsLock.Lock()
	c, ok := t.clients[target]
	if !ok {
		c = &client{}
		c.lock.Lock()
		t.clients[target] = c
	}
	t.clientsLock.Unlock()
	if ok {
		c.lock.Lock()
	}
	defer c.lock.Unlock()
	if c.c == nil {
		conn, err := grpc.Dial(string(target), t.dialOptions...)
		if err != nil {
			return nil, err
		}
		c.c = raftpb.NewRaftClient(conn)
	}
	return c.c, nil
}

type appendPipeline struct {
	stream         raftpb.Raft_AppendEntriesPipelineClient
	cancel         func()
	inflightChLock sync.Mutex
	inflightCh     chan *appendFuture
	doneCh         chan raft.AppendFuture
	shutdown       bool
}

func newAppendPipeline(client raftpb.RaftClient) *appendPipeline {
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)
	stream, err := client.AppendEntriesPipeline(ctx)
	if err != nil {
		cancel()
		return nil
	}
	ap := &appendPipeline{
		stream:     stream,
		cancel:     cancel,
		inflightCh: make(chan *appendFuture, 20),
		doneCh:     make(chan raft.AppendFuture, 20),
	}
	go ap.receiver()
	return ap
}

func (r *appendPipeline) AppendEntries(req *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	af := &appendFuture{
		start:   time.Now(),
		request: req,
		done:    make(chan struct{}),
	}
	if err := r.stream.Send(encodeAppendEntriesRequest(req)); err != nil {
		return nil, err
	}

	select {
	case <-r.stream.Context().Done():
		return nil, raft.ErrPipelineShutdown
	case r.inflightCh <- af:
		return af, nil
	}
}

func (r *appendPipeline) Consumer() <-chan raft.AppendFuture {
	return r.doneCh
}

func (r *appendPipeline) Close() error {
	r.inflightChLock.Lock()
	defer r.inflightChLock.Unlock()
	if r.shutdown {
		return nil
	}
	r.cancel()
	return nil
}

func (r *appendPipeline) receiver() {
	for af := range r.inflightCh {
		msg, err := r.stream.Recv()
		if err != nil {
			af.err = err
		} else {
			af.response = decodeAppendEntriesResponse(msg)
		}
		close(af.done)
		r.doneCh <- af
	}
}

type appendFuture struct {
	start    time.Time
	request  *raft.AppendEntriesRequest
	response *raft.AppendEntriesResponse
	err      error
	done     chan struct{}
}

func (f *appendFuture) Error() error {
	<-f.done
	return f.err
}

func (f *appendFuture) Start() time.Time {
	return f.start
}

func (f *appendFuture) Request() *raft.AppendEntriesRequest {
	return f.request
}

func (f *appendFuture) Response() *raft.AppendEntriesResponse {
	return f.response
}

func (t *gRPCTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	c, err := t.getClient(target)
	if err != nil {
		return nil, err
	}
	return newAppendPipeline(c), nil
}

func (t *gRPCTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	c, err := t.getClient(target)
	if err != nil {
		return err
	}
	ret, err := c.AppendEntries(context.TODO(), encodeAppendEntriesRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeAppendEntriesResponse(ret)
	return nil
}

func (t *gRPCTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	c, err := t.getClient(target)
	if err != nil {
		return err
	}
	ret, err := c.RequestVote(context.TODO(), encodeRequestVoteRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeRequestVoteResponse(ret)
	return nil
}

func (t *gRPCTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	c, err := t.getClient(target)
	if err != nil {
		return err
	}
	stream, err := c.InstallSnapshot(context.TODO())
	if err != nil {
		return err
	}
	if err := stream.Send(encodeInstallSnapshotRequest(args)); err != nil {
		return err
	}
	var buf [16384]byte
	for {
		n, err := data.Read(buf[:])
		if err == io.EOF || (err == nil && n == 0) {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&raftpb.InstallSnapshotRequest{
			Data: buf[:n],
		}); err != nil {
			return err
		}
	}
	ret, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	*resp = *decodeInstallSnapshotResponse(ret)
	return nil
}

func (t *gRPCTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	c, err := t.getClient(target)
	if err != nil {
		return err
	}
	ret, err := c.TimeoutNow(context.TODO(), encodeTimeoutNowRequest(args))
	if err != nil {
		return err
	}
	*resp = *decodeTimeoutNowResponse(ret)
	return nil
}

func (t *gRPCTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (t *gRPCTransport) DecodePeer(p []byte) raft.ServerAddress {
	return raft.ServerAddress(p)
}

func (t *gRPCTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	t.heartbeatFnLock.Lock()
	t.heartbeatFn = cb
	t.heartbeatFnLock.Unlock()
}

func (t *gRPCTransport) Register(s *grpc.Server) {
	raftpb.RegisterRaftServer(s, &raftAPI{t: t})
}
