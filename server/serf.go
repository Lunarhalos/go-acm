package server

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

const (
	// StatusReap is used to update the status of a node if we
	// are handling a EventMemberReap
	StatusReap = serf.MemberStatus(-1)
)

// setupServer is used to create the agent we use
func (s *ACMServer) setupSerf() (err error) {
	var (
		bindIP        string
		bindPort      int
		advertiseIP   string
		advertisePort int
	)
	// Get the address

	bindIP, bindPort, err = addrParts(s.config.ListenPeerUrls)
	if err != nil {
		s.logger.Errorf(fmt.Sprintf("invalid bind address: %s", err))
		return err
	}

	advertiseIP, advertisePort, err = addrParts(s.config.AdvertisePeerUrls)
	if err != nil {
		s.logger.Errorf(fmt.Sprintf("invalid advertise address: %s", err))
		return err
	}

	serfConfig := serf.DefaultConfig()
	switch s.config.Profile {
	case "lan":
		serfConfig.MemberlistConfig = memberlist.DefaultLANConfig()
	case "wan":
		serfConfig.MemberlistConfig = memberlist.DefaultWANConfig()
	case "local":
		serfConfig.MemberlistConfig = memberlist.DefaultLocalConfig()
	default:
		s.logger.Errorf(fmt.Sprintf("unknown profile: %s", s.config.Profile))
		return fmt.Errorf("unknown profile: %s", s.config.Profile)
	}

	serfConfig.MemberlistConfig.BindAddr = bindIP
	serfConfig.MemberlistConfig.BindPort = bindPort
	serfConfig.MemberlistConfig.AdvertiseAddr = advertiseIP
	serfConfig.MemberlistConfig.AdvertisePort = advertisePort
	serfConfig.NodeName = s.config.NodeName
	tags := make(map[string]string)
	tags[TagRPCAddr] = s.config.AdvertiseClientUrls
	serfConfig.Tags = tags
	serfConfig.CoalescePeriod = 3 * time.Second
	serfConfig.QuiescentPeriod = time.Second
	serfConfig.UserCoalescePeriod = 3 * time.Second
	serfConfig.UserQuiescentPeriod = time.Second
	serfConfig.ReconnectTimeout = s.config.ReconnectTimeout

	serfEventCh := make(chan serf.Event, 2048)
	serfConfig.EventCh = serfEventCh

	if s.logger.Logger.Level == logrus.DebugLevel {
		serfConfig.LogOutput = s.logger.Logger.Writer()
		serfConfig.MemberlistConfig.LogOutput = s.logger.Logger.Writer()
	} else {
		serfConfig.LogOutput = ioutil.Discard
		serfConfig.MemberlistConfig.LogOutput = ioutil.Discard
	}

	// Create serf first
	s.serf, err = serf.Create(serfConfig)
	if err != nil {
		s.logger.Error(err)
		return err
	}

	// 监听serf事件
	go func() {
		for {
			select {
			case e := <-serfEventCh:
				s.logger.Infof("acm: received event: %s", e.String())
				if me, ok := e.(serf.MemberEvent); ok {
					s.handleMemberEvent(me)
				}
			case <-s.serf.ShutdownCh():
				s.logger.Warn("agent: Serf shutdown detected, quitting")
				return
			}
		}
	}()
	return nil
}

func (s *ACMServer) handleMemberEvent(me serf.MemberEvent) {
	// Do nothing if we are not the leader
	if s.raft.State() != raft.Leader {
		return
	}

	// Check if this is a reap event
	isReap := me.EventType() == serf.EventMemberReap

	// Queue the members for reconciliation
	for _, m := range me.Members {
		// Change the status if this is a reap event
		if isReap {
			m.Status = StatusReap
		}
		select {
		case s.reconcileCh <- m:
		default:
		}
	}
}

func (s *ACMServer) addNode(node *node) error {
	s.nodeLock.Lock()
	s.nodes[node.addr.String()] = node
	s.nodeLock.Unlock()
	return s.addRaftNode(node)
}

func (s *ACMServer) removeNode(node *node) error {
	s.nodeLock.Lock()
	delete(s.nodes, node.addr.String())
	s.nodeLock.Unlock()
	return s.removeNode(node)
}

// Join asks the Serf instance to join. See the Serf.Join function.
func (s *ACMServer) join(addrs []string, replay bool) (n int, err error) {
	s.logger.Infof("agent: joining: %v replay: %v", addrs, replay)
	n, err = s.serf.Join(addrs, !replay)
	if n > 0 {
		s.logger.Infof("acm: joined: %d nodes", n)
	}
	if err != nil {
		s.logger.Warnf("acm: error joining: %v", err)
	}
	return
}
