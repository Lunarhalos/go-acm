package internal

import (
	"net"

	"github.com/Lunarhalos/go-acm/api/acmserverpb"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

type Node struct {
	ID      string
	Name    string
	Addr    net.Addr
	RPCAddr net.Addr
	Status  serf.MemberStatus
}

func parseNode(member serf.Member) *Node {
	node := &Node{
		ID:     member.Name,
		Name:   member.Name,
		Addr:   &net.TCPAddr{IP: member.Addr, Port: int(member.Port)},
		Status: member.Status,
	}
	rpcAddr := net.ParseIP(member.Tags[""])
	return node
}

type Server struct {
	client  acmserverpb.ACMClient
	storage Storage
	serf    *serf.Serf
	eventCh chan serf.Event
	raft    *raft.Raft
	nodes   map[string]*Node
	conf    *serf.Config
}

func NewServer() {

}

func (s *Server) Run() error {
	sf, err := serf.Create(s.conf)
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
			s.addNode(node)
		case serf.StatusLeaving:
			s.removeNode(node)
		}
	}
}

func (s *Server) addNode(node *Node) {

}

func (s *Server) removeNode(node *Node) {

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
			future := s.raft.RemoveServer(raft.ServerID(node.id), 0, 0)
			if err := future.Error(); err != nil {
				return err
			}
			break
		}
	}
	return nil
}
