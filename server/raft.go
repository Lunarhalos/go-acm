package server

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Lunarhalos/go-acm/pkg/hashicorp-raft/transport"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	raftTimeout = 30 * time.Second
	// raftLogCacheSize is the maximum number of logs to cache in-memory.
	// This is used to reduce disk I/O for the recently committed entries.
	raftLogCacheSize = 512

	barrierWriteTimeout = 2 * time.Minute
)

type node struct {
	id      string
	name    string
	addr    net.Addr
	rpcAddr net.Addr
	Status  serf.MemberStatus
}

func parseNode(member serf.Member) *node {
	node := &node{
		id:     member.Name,
		name:   member.Name,
		addr:   &net.TCPAddr{IP: member.Addr, Port: int(member.Port)},
		Status: member.Status,
	}
	node.rpcAddr, _ = net.ResolveTCPAddr("tcp", member.Tags[TagRPCAddr])
	return node
}

func (s *ACMServer) setupRaft() error {
	logger := ioutil.Discard
	if s.logger.Logger.Level == logrus.DebugLevel {
		logger = s.logger.Logger.Writer()
	}

	s.raftTransport = transport.NewGRPCTransport(raft.ServerAddress(s.config.AdvertiseClientUrls), grpc.WithInsecure())

	raftConfig := raft.DefaultConfig()
	// Raft performance
	raftMultiplier := s.config.RaftMultiplier
	if raftMultiplier < 1 || raftMultiplier > 10 {
		return fmt.Errorf("raft-multiplier cannot be %d. Must be between 1 and 10", raftMultiplier)
	}
	raftConfig.HeartbeatTimeout = raftConfig.HeartbeatTimeout * time.Duration(raftMultiplier)
	raftConfig.ElectionTimeout = raftConfig.ElectionTimeout * time.Duration(raftMultiplier)
	raftConfig.LeaderLeaseTimeout = raftConfig.LeaderLeaseTimeout * time.Duration(s.config.RaftMultiplier)
	raftConfig.LocalID = raft.ServerID(s.config.NodeName)

	// Build an all in-memory setup for dev mode, otherwise prepare a full
	// disk-based setup.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	var snapshots raft.SnapshotStore
	var err error
	// Create the snapshot store. This allows the Raft to truncate the log to
	// mitigate the issue of having an unbounded replicated log.
	snapshots, err = raft.NewFileSnapshotStore(filepath.Join(s.config.DataDir, "raft"), 3, logger)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the BoltDB backend
	raftStore, err := raftboltdb.NewBoltStore(filepath.Join(s.config.DataDir, "raft", "raft.db"))
	if err != nil {
		return fmt.Errorf("error creating new raft store: %s", err)
	}
	s.raftStore = raftStore
	stableStore = raftStore

	// Wrap the store in a LogCache to improve performance
	cacheStore, err := raft.NewLogCache(raftLogCacheSize, raftStore)
	if err != nil {
		raftStore.Close()
		return err
	}
	logStore = cacheStore

	// Check for peers.json file for recovery
	peersFile := filepath.Join(s.config.DataDir, "raft", "peers.json")
	if _, err := os.Stat(peersFile); err == nil {
		s.logger.Info("found peers.json file, recovering Raft configuration...")
		var configuration raft.Configuration
		configuration, err = raft.ReadConfigJSON(peersFile)
		if err != nil {
			return fmt.Errorf("recovery failed to parse peers.json: %v", err)
		}
		store, err := NewStore(s.logger)
		if err != nil {
			s.logger.WithError(err).Fatal("acm: Error initializing store")
		}
		tmpFsm := newFSM(store, s.logger)
		if err := raft.RecoverCluster(raftConfig, tmpFsm,
			logStore, stableStore, snapshots, s.raftTransport, configuration); err != nil {
			return fmt.Errorf("recovery failed: %v", err)
		}
		if err := os.Remove(peersFile); err != nil {
			return fmt.Errorf("recovery failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
		}
		s.logger.Info("deleted peers.json file after successful recovery")
	}

	if len(s.config.StartJoin) == 0 {
		// If we are in bootstrap or dev mode and the state is clean then we can
		// bootstrap now.
		hasState, err := raft.HasExistingState(logStore, stableStore, snapshots)
		if err != nil {
			return err
		}
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raftConfig.LocalID,
						Address: s.raftTransport.LocalAddr(),
					},
				},
			}
			if err := raft.BootstrapCluster(raftConfig, logStore, stableStore, snapshots, s.raftTransport, configuration); err != nil {
				return err
			}
		}
	}

	fsm := newFSM(s.store, s.logger)
	rft, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshots, s.raftTransport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = rft
	return nil
}

func (s *ACMServer) monitorLeadership() {
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		s.logger.Info("acm: monitoring leadership")
		select {
		case isLeader := <-s.raft.LeaderCh():
			switch {
			case isLeader:
				if weAreLeaderCh != nil {
					s.logger.Error("acm: attempted to start the leader loop while running")
					continue
				}

				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					s.leaderLoop(ch)
				}(weAreLeaderCh)
				s.logger.Info("acm: cluster leadership acquired")

			default:
				if weAreLeaderCh == nil {
					s.logger.Error("acm: attempted to stop the leader loop while not running")
					continue
				}

				s.logger.Debug("acm: shutting down leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				s.logger.Info("acm: cluster leadership lost")
			}

		case <-s.shutdownCh:
			return
		}
	}
}

// leaderLoop runs as long as we are the leader to run various
// maintenance activities
func (s *ACMServer) leaderLoop(stopCh chan struct{}) {
	var reconcileCh chan serf.Member
RECONCILE:
	// Setup a reconciliation timer
	reconcileCh = nil
	interval := time.After(s.config.ReconcileInterval)

	// Apply a raft barrier to ensure our FSM is caught up
	barrier := s.raft.Barrier(barrierWriteTimeout)
	if err := barrier.Error(); err != nil {
		s.logger.WithError(err).Error("acm: failed to wait for barrier")
		goto WAIT
	}

	// Reconcile any missing data
	if err := s.reconcile(); err != nil {
		s.logger.WithError(err).Error("acm: failed to reconcile")
		goto WAIT
	}

	// Initial reconcile worked, now we can process the channel
	// updates
	reconcileCh = s.reconcileCh

	// Poll the stop channel to give it priority so we don't waste time
	// trying to perform the other operations if we have been asked to shut
	// down.
	select {
	case <-stopCh:
		return
	default:
	}

WAIT:
	// Wait until leadership is lost
	for {
		s.logger.Info("read reconcileCh")
		select {
		case <-stopCh:
			return
		case <-s.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			s.reconcileMember(member)
		}
	}
}

// reconcile is used to reconcile the differences between Serf
// membership and what is reflected in our strongly consistent store.
func (s *ACMServer) reconcile() error {
	members := s.serf.Members()
	for _, member := range members {
		if err := s.reconcileMember(member); err != nil {
			return err
		}
	}
	return nil
}

// reconcileMember is used to do an async reconcile of a single serf member
func (s *ACMServer) reconcileMember(member serf.Member) error {
	// Check if this is a member we should handle
	node := parseNode(member)
	s.logger.Info(member.Status)
	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = s.addRaftNode(node)
	case serf.StatusLeft:
		err = s.removeRaftNode(node)
	}
	if err != nil {
		s.logger.WithError(err).WithField("member", member).Error("failed to reconcile member")
		return err
	}
	return nil
}

func (s *ACMServer) addRaftNode(node *node) error {
	s.logger.Infof("acm: add raft node %s, %s", node.id, node.addr.String())
	cfg := s.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return err
	}

	if node.name == s.config.NodeName {
		return nil
	}
	future := s.raft.AddVoter(raft.ServerID(node.id), raft.ServerAddress(node.rpcAddr.String()), 0, 0)
	if err := future.Error(); err != nil {
		return err
	}
	return nil
}

func (s *ACMServer) removeRaftNode(node *node) error {
	s.logger.Infof("acm: remove raft node %s, %s", node.id, node.addr.String())
	cfg := s.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return err
	}
	for _, server := range cfg.Configuration().Servers {
		if server.ID == raft.ServerID(node.id) {
			future := s.raft.RemoveServer(raft.ServerID(node.id), 0, 0)
			if err := future.Error(); err != nil {
				return err
			}
			break
		}
	}
	return nil
}
