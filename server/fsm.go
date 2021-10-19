package server

import (
	"bytes"
	"io"

	"github.com/Lunarhalos/go-acm/api/acmserverpb"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// MessageType is the type to encode FSM commands.
type MessageType uint8

const (
	MessageTypeSave MessageType = iota
	MessageTypeDelete
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
	case MessageTypeSave:
		return fsm.applySaveEntry(buf[1:])
	case MessageTypeDelete:
		return fsm.applyDeleteEntry(buf[1:])
	}
	return nil
}

func (fsm *acmFSM) applySaveEntry(buf []byte) interface{} {
	var pe acmserverpb.ConfigEntry
	if err := proto.Unmarshal(buf, &pe); err != nil {
		return err
	}
	e := &ConfigEntry{
		Namespace: pe.Namespace,
		Name:      pe.Name,
		Data:      []byte(pe.Data),
	}
	if err := fsm.store.Save(e); err != nil {
		return err
	}
	return nil
}

func (fsm *acmFSM) applyDeleteEntry(buf []byte) interface{} {
	var pdr acmserverpb.DeleteRequest
	if err := proto.Unmarshal(buf, &pdr); err != nil {
		return err
	}
	err := fsm.store.Delete(pdr.Namespace, pdr.Name)
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
