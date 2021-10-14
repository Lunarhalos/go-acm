package server

import (
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"

	"github.com/Lunarhalos/go-acm/api/acmserverpb"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

type Node struct {
	ID        string
	Name      string
	Addr      net.Addr
	RPCAddr   net.Addr
	RPCClient acmserverpb.ACMClient
	Status    serf.MemberStatus
}

func parseNode(member serf.Member) *Node {
	node := &Node{
		ID:     member.Name,
		Name:   member.Name,
		Addr:   &net.TCPAddr{IP: member.Addr, Port: int(member.Port)},
		Status: member.Status,
	}
	rpcAddr := net.ParseIP(member.Tags[TagRPCAddr])
	rpcPort, _ := strconv.Atoi(member.Tags[TagRPCPort])
	node.RPCAddr = &net.TCPAddr{IP: rpcAddr, Port: rpcPort}
	return node
}

type Server struct {
	client   acmserverpb.ACMClient
	storage  Storage
	serf     *serf.Serf
	eventCh  chan serf.Event
	raft     *raft.Raft
	nodes    map[string]*Node
	config   *Config
	nodeLock sync.RWMutex
}

func NewServer() *Server {
	s := &Server{
		nodes:    make(map[string]*Node),
		config:   nil,
		nodeLock: sync.RWMutex{},
	}
	return s
}

func (s *Server) Run() error {
	sf, err := serf.Create(nil)
	if err != nil {
		return err
	}
	s.serf = sf
	go s.eventLoop()
	return nil
}

func (s *Server) eventLoop() {
	for {
		select {
		case e := <-s.eventCh:
			if me, ok := e.(serf.MemberEvent); ok {
				s.handleMemberEvent(me)
			}
		}
	}
}

func (s *Server) handleMemberEvent(me serf.MemberEvent) {
	if s.raft.State() != raft.Leader {
		return
	}

	for _, member := range me.Members {
		node := parseNode(member)
		switch member.Status {
		case serf.StatusAlive:
			if err := s.addNode(node); err != nil {

			}
		case serf.StatusLeaving:
			if err := s.removeNode(node); err != nil {

			}
		}
	}
}

// setupSerf is used to create the agent we use
func (s *Server) setupSerf() (*serf.Serf, error) {
	config := s.config

	bindIP, bindPort, err := config.AddrParts(config.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid bind address: %s", err)
	}

	var advertiseIP string
	var advertisePort int
	if config.AdvertiseAddr != "" {
		advertiseIP, advertisePort, err = config.AddrParts(config.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid advertise address: %s", err)
		}
	}

	encryptKey, err := config.EncryptBytes()
	if err != nil {
		return nil, fmt.Errorf("invalid encryption key: %s", err)
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.Init()

	serfConfig.Tags = a.config.Tags
	serfConfig.Tags["role"] = "dkron"
	serfConfig.Tags["dc"] = a.config.Datacenter
	serfConfig.Tags["region"] = a.config.Region
	serfConfig.Tags["version"] = Version
	if a.config.Server {
		serfConfig.Tags["server"] = strconv.FormatBool(a.config.Server)
	}
	if a.config.Bootstrap {
		serfConfig.Tags["bootstrap"] = "1"
	}
	if a.config.BootstrapExpect != 0 {
		serfConfig.Tags["expect"] = fmt.Sprintf("%d", a.config.BootstrapExpect)
	}

	switch config.Profile {
	case "lan":
		serfConfig.MemberlistConfig = memberlist.DefaultLANConfig()
	case "wan":
		serfConfig.MemberlistConfig = memberlist.DefaultWANConfig()
	case "local":
		serfConfig.MemberlistConfig = memberlist.DefaultLocalConfig()
	default:
		return nil, fmt.Errorf("unknown profile: %s", config.Profile)
	}

	serfConfig.MemberlistConfig.BindAddr = bindIP
	serfConfig.MemberlistConfig.BindPort = bindPort
	serfConfig.MemberlistConfig.AdvertiseAddr = advertiseIP
	serfConfig.MemberlistConfig.AdvertisePort = advertisePort
	serfConfig.MemberlistConfig.SecretKey = encryptKey
	serfConfig.NodeName = config.NodeName
	serfConfig.Tags = config.Tags
	serfConfig.CoalescePeriod = 3 * time.Second
	serfConfig.QuiescentPeriod = time.Second
	serfConfig.UserCoalescePeriod = 3 * time.Second
	serfConfig.UserQuiescentPeriod = time.Second
	serfConfig.ReconnectTimeout, err = time.ParseDuration(config.SerfReconnectTimeout)

	if err != nil {
		a.logger.Fatal(err)
	}

	// Create a channel to listen for events from Serf
	a.eventCh = make(chan serf.Event, 2048)
	serfConfig.EventCh = a.eventCh

	// Start Serf
	a.logger.Info("agent: Dkron agent starting")

	if a.logger.Logger.Level == logrus.DebugLevel {
		serfConfig.LogOutput = a.logger.Logger.Writer()
		serfConfig.MemberlistConfig.LogOutput = a.logger.Logger.Writer()
	} else {
		serfConfig.LogOutput = ioutil.Discard
		serfConfig.MemberlistConfig.LogOutput = ioutil.Discard
	}

	// Create serf first
	serf, err := serf.Create(serfConfig)
	if err != nil {
		a.logger.Error(err)
		return nil, err
	}
	return serf, nil
}

func (s *Server) addNode(node *Node) error {
	s.nodeLock.Lock()
	s.nodes[node.Addr.String()] = node
	s.nodeLock.Unlock()
	return s.addRaftNode(node)
}

func (s *Server) removeNode(node *Node) error {
	s.nodeLock.Lock()
	delete(s.nodes, node.Addr.String())
	s.nodeLock.Unlock()
	return s.removeNode(node)
}

func (s *Server) addRaftNode(node *Node) error {
	cfg := s.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return err
	}

	if node.Name == s.conf.NodeName {
		return nil
	}
	future := s.raft.AddVoter(raft.ServerID(node.ID), raft.ServerAddress(node.Addr.String()), 0, 0)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

func (s *Server) removeRaftNode(node *Node) error {
	cfg := s.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return err
	}
	for _, server := range cfg.Configuration().Servers {
		if server.ID == raft.ServerID(node.ID) {
			future := s.raft.RemoveServer(raft.ServerID(node.ID), 0, 0)
			if err := future.Error(); err != nil {
				return err
			}
			break
		}
	}
	return nil
}
