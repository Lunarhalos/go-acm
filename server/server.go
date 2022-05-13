package server

import (
	"context"
	"net"
	"sync"

	pb "github.com/Lunarhalos/go-acm/api/acmserverpb"
	"github.com/Lunarhalos/go-acm/pkg/filter"
	"github.com/Lunarhalos/go-acm/pkg/hashicorp-raft/transport"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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

	pb.UnimplementedACMServer
}

func New(config *Config) *ACMServer {
	config = populateConfig(config)
	s := &ACMServer{
		nodes:       make(map[string]*node),
		config:      config,
		logger:      InitLogger(config.LogLevel, config.NodeName),
		reconcileCh: make(chan serf.Member),
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

	if len(s.config.StartJoin) > 0 {
		// start join
		s.join(s.config.StartJoin, true)
	}

	if err := s.ServeGRPC(); err != nil {
		s.logger.WithError(err).Fatal("server failed to start")
	}

	if err := s.ServeHTTP(); err != nil {
		s.logger.WithError(err).Fatal("server failed to start")
	}

	go s.monitorLeadership()

	return nil
}

func (s *ACMServer) Stop() error {
	s.logger.Info("agent: Called member stop, now stopping")

	if err := s.serf.Leave(); err != nil {
		return err
	}

	if err := s.serf.Shutdown(); err != nil {
		return err
	}

	future := s.raft.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}

	if err := s.store.Shutdown(); err != nil {
		return err
	}

	return nil
}

func (s *ACMServer) CreateNamespace(ctx context.Context, request *pb.CreateNamespaceRequest) (*pb.CreateNamespaceResponse, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	namespace := &pb.Namespace{
		Name:        request.Name,
		NamespaceId: id.String(),
	}
	cmd, err := Encode(MessageTypeCreateNamespace, namespace)
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
	return &pb.CreateNamespaceResponse{NamespaceId: id.String()}, nil
}
func (s *ACMServer) ListNamespaces(context.Context, *pb.ListNamespacesRequest) (*pb.ListNamespacesResponse, error) {
	namespaces, err := s.store.ListNamespaces()
	if err != nil {
		return nil, err
	}
	response := &pb.ListNamespacesResponse{
		Namespaces: namespaces,
	}
	return response, nil
}

func (s *ACMServer) GetNamespace(ctx context.Context, request *pb.GetNamespaceRequest) (*pb.GetNamespaceResponse, error) {
	namespace, err := s.store.GetNamespace(request.NamespaceId)
	if err != nil {
		return nil, err
	}
	response := &pb.GetNamespaceResponse{
		Namespace: namespace,
	}
	return response, nil
}

func (s *ACMServer) DeleteNamespace(ctx context.Context, request *pb.DeleteNamespaceRequest) (*pb.DeleteNamespaceResponse, error) {
	cmd, err := Encode(MessageTypeDeleteNamespace, request)
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
	return &pb.DeleteNamespaceResponse{}, nil
}

func (s *ACMServer) CreateConfiguration(ctx context.Context, request *pb.CreateConfigurationRequest) (*pb.CreateConfigurationResponse, error) {
	configuration := &pb.Configuration{
		NamespaceId: request.NamespaceId,
		DataId:      request.DataId,
		Group:       request.Group,
		Content:     request.Content,
	}
	cmd, err := Encode(MessageTypeCreateConfiguration, configuration)
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
	return &pb.CreateConfigurationResponse{}, nil
}

func (s *ACMServer) ListConfigurations(ctx context.Context, request *pb.ListConfigurationsRequest) (*pb.ListConfigurationsResponse, error) {
	es, err := s.store.ListConfigurations(request.NamespaceId)
	if err != nil {
		return nil, err
	}
	response := &pb.ListConfigurationsResponse{
		Configurations: es,
	}

	return response, nil
}

func (s *ACMServer) GetConfiguration(ctx context.Context, request *pb.GetConfigurationRequest) (*pb.GetConfigurationResponse, error) {
	e, err := s.store.GetConfiguration(request.NamespaceId, request.Group, request.DataId)
	if err != nil {
		return nil, err
	}
	response := &pb.GetConfigurationResponse{
		Configuration: e,
	}
	return response, nil
}

func (s *ACMServer) ListHistoryConfigurations(ctx context.Context, request *pb.ListHistoryConfigurationsRequest) (*pb.ListHistoryConfigurationsResponse, error) {
	es, err := s.store.ListHistoryConfigurations(request.NamespaceId, request.Group, request.DataId)
	if err != nil {
		return nil, err
	}
	response := &pb.ListHistoryConfigurationsResponse{
		Configurations: es,
	}
	return response, nil
}

func (s *ACMServer) DeleteConfiguration(ctx context.Context, request *pb.DeleteConfigurationRequest) (*pb.DeleteConfigurationResponse, error) {
	cmd, err := Encode(MessageTypeDeleteConfiguration, request)
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
	return &pb.DeleteConfigurationResponse{}, nil
}

func (s *ACMServer) ServeGRPC() error {
	lis, err := net.Listen("tcp", s.config.ListenClientUrls)
	if err != nil {
		s.logger.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterACMServer(grpcServer, s)
	s.raftTransport.Register(grpcServer)
	go grpcServer.Serve(lis)
	return nil
}

func (s *ACMServer) ServeHTTP() error {
	httpServer := http.NewServer(http.Address(s.config.HTTPAddr), http.Network("tcp"), http.Filter(filter.CORS()))
	pb.RegisterACMHTTPServer(httpServer, s)
	go httpServer.Start(context.Background())
	return nil
}
