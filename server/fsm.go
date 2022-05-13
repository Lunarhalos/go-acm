package server

import (
	"bytes"
	"io"

	pb "github.com/Lunarhalos/go-acm/api/acmserverpb"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// MessageType is the type to encode FSM commands.
type MessageType uint8

const (
	MessageTypeCreateNamespace MessageType = iota
	MessageTypeDeleteNamespace
	MessageTypeCreateConfiguration
	MessageTypeDeleteConfiguration
)

// Encode is used to encode a Protoc object with type prefix
func Encode(t MessageType, msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	m, err := proto.Marshal(msg.(proto.Message))
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(m)
	return buf.Bytes(), err
}

type acmFSM struct {
	store  Storage
	logger *logrus.Entry
}

func newFSM(store Storage, logger *logrus.Entry) *acmFSM {
	return &acmFSM{
		store:  store,
		logger: logger,
	}
}

func (fsm *acmFSM) Apply(l *raft.Log) interface{} {
	buf := l.Data
	msgType := MessageType(buf[0])

	fsm.logger.WithField("command", msgType).Debug("fsm: received command")

	switch msgType {
	case MessageTypeCreateNamespace:
		return fsm.applyCreateNamespace(buf[1:])
	case MessageTypeDeleteNamespace:
		return fsm.applyDeleteNamespace(buf[1:])
	case MessageTypeCreateConfiguration:
		return fsm.applyCreateConfiguration(buf[1:])
	case MessageTypeDeleteConfiguration:
		return fsm.applyDeleteConfiguration(buf[1:])
	}
	return nil
}

func (fsm *acmFSM) applyCreateNamespace(buf []byte) interface{} {
	var namespace pb.Namespace
	if err := proto.Unmarshal(buf, &namespace); err != nil {
		return err
	}
	if err := fsm.store.CreateNamespace(&namespace); err != nil {
		return err
	}
	return nil
}

func (fsm *acmFSM) applyDeleteNamespace(buf []byte) interface{} {
	var pdr pb.DeleteNamespaceRequest
	if err := proto.Unmarshal(buf, &pdr); err != nil {
		return err
	}
	err := fsm.store.DeleteNamespace(pdr.NamespaceId)
	if err != nil {
		return err
	}
	return nil
}

func (fsm *acmFSM) applyCreateConfiguration(buf []byte) interface{} {
	var configuration pb.Configuration
	if err := proto.Unmarshal(buf, &configuration); err != nil {
		return err
	}

	if err := fsm.store.CreateConfiguration(&configuration); err != nil {
		return err
	}
	return nil
}

func (fsm *acmFSM) applyDeleteConfiguration(buf []byte) interface{} {
	var pdr pb.DeleteConfigurationRequest
	if err := proto.Unmarshal(buf, &pdr); err != nil {
		return err
	}
	err := fsm.store.DeleteConfiguration(pdr.NamespaceId, pdr.Group, pdr.DataId)
	if err != nil {
		return err
	}
	return nil
}

func (fsm *acmFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &acmSnapshot{store: fsm.store}, nil
}

func (fsm *acmFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return fsm.store.Restore(rc)
}

type acmSnapshot struct {
	store Storage
}

func (a *acmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := a.store.Snapshot(sink); err != nil {
		sink.Cancel()
		return err
	}

	// Close the sink.
	if err := sink.Close(); err != nil {
		return err
	}

	return nil
}

func (a *acmSnapshot) Release() {}
