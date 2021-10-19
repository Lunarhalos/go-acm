package server

import (
	"context"
	"net"
	"sync"

	"github.com/Lunarhalos/go-acm/pkg/raft/transport"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/Lunarhalos/go-acm/api/acmserverpb"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

type ACMServer struct {
	storage Storage

	serf        *serf.Serf
	reconcileCh chan serf.Member

	raft          *raft.Raft
	raftTransport transport.GRPCTransport
	raftStore     raft.LogStore
	store         Storage

	nodes    map[string]*node
	config   *Config
	nodeLock sync.RWMutex
	logger   *logrus.Entry

	shutdownCh chan struct{}

	acmserverpb.UnimplementedACMServer
}

func New(config *Config) *ACMServer {
	config = populateConfig(config)
	s := &ACMServer{
		nodes:  make(map[string]*node),
		config: config,
		logger: InitLogger(config.LogLevel, config.NodeName),
	}

	return s
}

func (s *ACMServer) Start() error {
	store, err := NewStore(s.logger)
	if err != nil {
		return err
	}
	s.store = store

	if err := s.setupSerf(); err != nil {
		return err
	}

	if err := s.setupRaft(); err != nil {
		return err
	}

	// start join
	s.join(s.config.StartJoin, true)

	l, err := net.Listen("tcp", s.config.ListenClientUrls)
	if err != nil {
		s.logger.Fatal(err)
	}

	if err := s.Serve(l); err != nil {
		s.logger.WithError(err).Fatal("server failed to start")
	}

	return nil
}

func (s *ACMServer) Stop() error {
	s.logger.Info("agent: Called member stop, now stopping")

	s.raft.Shutdown()

	if err := s.serf.Leave(); err != nil {
		return err
	}

	if err := s.serf.Shutdown(); err != nil {
		return err
	}

	return nil
}

func (s *ACMServer) Get(ctx context.Context, request *acmserverpb.GetRequest) (*acmserverpb.GetResponse, error) {
	e, err := s.store.Get(request.Namespace, request.Name)
	if err != nil {
		return nil, err
	}
	response := &acmserverpb.GetResponse{
		Entry: &acmserverpb.ConfigEntry{
			Namespace: e.Namespace,
			Name:      e.Name,
			Data:      string(e.Data),
		},
	}
	return response, nil
}

func (s *ACMServer) Save(ctx context.Context, request *acmserverpb.SaveRequest) (*acmserverpb.SaveResponse, error) {
	cmd, err := Encode(MessageTypeSave, request.Entry)
	if err != nil {
		return nil, err
	}
	af := s.raft.Apply(cmd, raftTimeout)
	if err := af.Error(); err != nil {
		return nil, err
	}

	switch v := af.Response().(type) {
	case error:
		return nil, v
	}
	return &acmserverpb.SaveResponse{}, nil
}

func (s *ACMServer) History(ctx context.Context, request *acmserverpb.HistoryRequest) (*acmserverpb.HistoryResponse, error) {
	es, err := s.store.History(request.Namespace, request.Name)
	if err != nil {
		return nil, err
	}
	response := &acmserverpb.HistoryResponse{}
	for _, e := range es {
		response.Entries = append(response.Entries, &acmserverpb.ConfigEntry{
			Namespace: e.Namespace,
			Name:      e.Name,
			Data:      string(e.Data),
		})
	}
	return response, nil
}

func (s *ACMServer) Delete(ctx context.Context, request *acmserverpb.DeleteRequest) (*acmserverpb.DeleteResponse, error) {
	cmd, err := Encode(MessageTypeDelete, request)
	if err != nil {
		return nil, err
	}
	af := s.raft.Apply(cmd, raftTimeout)
	if err := af.Error(); err != nil {
		return nil, err
	}

	switch v := af.Response().(type) {
	case error:
		return nil, v
	}
	return &acmserverpb.DeleteResponse{}, nil
}

func (s *ACMServer) Serve(lis net.Listener) error {
	grpcServer := grpc.NewServer()
	acmserverpb.RegisterACMServer(grpcServer, s)
	s.raftTransport.Register(grpcServer)
	go grpcServer.Serve(lis)
	return nil
}
