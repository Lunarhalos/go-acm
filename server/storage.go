package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	pb "github.com/Lunarhalos/go-acm/api/acmserverpb"
	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
)

const (
	namespacePrefix     = "acm:namespace"
	configurationPrefix = "acm:configuration"
)

var (
	// ErrDuplicatedNamespace ...
	ErrDuplicatedNamespace = errors.New("duplicated namespace")
)

func newConfigurationKey(namespaceID, group, dataID string) string {
	return fmt.Sprintf("%s:%s:%s:%s", configurationPrefix, namespaceID, group, dataID)
}

func newNSConfigurationPrefix(namespaceID string) string {
	return fmt.Sprintf("%s:%s", configurationPrefix, namespaceID)
}

func newNamespaceKey(namespaceID string) string {
	return fmt.Sprintf("%s:%s", namespacePrefix, namespaceID)
}

type Storage interface {
	CreateNamespace(namespace *pb.Namespace) error
	ListNamespaces() ([]*pb.Namespace, error)
	GetNamespace(namespaceID string) (*pb.Namespace, error)
	DeleteNamespace(namespaceID string) error
	CreateConfiguration(configuration *pb.Configuration) error
	ListConfigurations(namespaceID string) ([]*pb.Configuration, error)
	GetConfiguration(namespaceID, group, dataID string) (*pb.Configuration, error)
	ListHistoryConfigurations(namespaceID, group, dataID string) ([]*pb.Configuration, error)
	DeleteConfiguration(namespaceID, group, dataID string) error
	Snapshot(w io.WriteCloser) error
	Restore(r io.ReadCloser) error
	Shutdown() error
}

type Store struct {
	db     *badger.DB
	logger *logrus.Entry
}

func NewStore(logger *logrus.Entry) (*Store, error) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		return nil, err
	}
	store := &Store{
		db:     db,
		logger: logger,
	}
	return store, nil
}

func (s *Store) CreateNamespace(namespace *pb.Namespace) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		var namespaceKey = newNamespaceKey(namespace.NamespaceId)
		// 查询是否已经存在
		_, err := txn.Get([]byte(namespaceKey))
		if err == nil {
			return ErrDuplicatedNamespace
		}

		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		data, _ := json.Marshal(namespace)
		if err := txn.Set([]byte(namespaceKey), data); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
func (s *Store) ListNamespaces() ([]*pb.Namespace, error) {
	var namespaces []*pb.Namespace
	err := s.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.Prefix = []byte(namespacePrefix)
		opt.AllVersions = false

		itr := txn.NewIterator(opt)
		defer itr.Close()

		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			if err := item.Value(func(val []byte) error {
				var namespace pb.Namespace
				if err := json.Unmarshal(val, &namespace); err != nil {
					return err
				}
				namespaces = append(namespaces, &namespace)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return namespaces, nil
}

func (s *Store) GetNamespace(namespaceID string) (*pb.Namespace, error) {
	var namespace pb.Namespace
	err := s.db.View(func(txn *badger.Txn) error {
		namespaceKey := newNamespaceKey(namespaceID)
		item, err := txn.Get([]byte(namespaceKey))
		if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &namespace); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &namespace, nil
}

func (s *Store) DeleteNamespace(namespaceID string) error {
	if err := s.db.Update(func(txn *badger.Txn) error {
		namespaceKey := newNamespaceKey(namespaceID)
		if err := txn.Delete([]byte(namespaceKey)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// CreateConfiguration 保存配置条目
func (s *Store) CreateConfiguration(configuration *pb.Configuration) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		configurationKey := newConfigurationKey(configuration.NamespaceId, configuration.Group, configuration.DataId)
		data, _ := json.Marshal(configuration)
		if err := txn.Set([]byte(configurationKey), data); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) ListConfigurations(namespaceID string) ([]*pb.Configuration, error) {
	var configurations []*pb.Configuration
	err := s.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.Prefix = []byte(newNSConfigurationPrefix(namespaceID))
		opt.AllVersions = false

		itr := txn.NewIterator(opt)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			if err := item.Value(func(val []byte) error {
				var configuration pb.Configuration
				if err := json.Unmarshal(val, &configuration); err != nil {
					return err
				}
				configuration.Version = item.Version()
				configurations = append(configurations, &configuration)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return configurations, nil
}

func (s *Store) GetConfiguration(namespaceID, group, dataID string) (*pb.Configuration, error) {
	var configuration pb.Configuration
	err := s.db.View(func(txn *badger.Txn) error {
		configurationKey := newConfigurationKey(namespaceID, group, dataID)
		item, err := txn.Get([]byte(configurationKey))
		if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &configuration); err != nil {
			return err
		}
		configuration.Version = item.Version()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &configuration, nil
}

func (s *Store) ListHistoryConfigurations(namespaceID, group, dataID string) ([]*pb.Configuration, error) {
	var configurations []*pb.Configuration
	err := s.db.View(func(txn *badger.Txn) error {
		configurationKey := newConfigurationKey(namespaceID, group, dataID)
		opt := badger.DefaultIteratorOptions
		itr := txn.NewKeyIterator([]byte(configurationKey), opt)
		defer itr.Close()
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			if err := item.Value(func(val []byte) error {
				var configuration pb.Configuration
				if err := json.Unmarshal(val, &configuration); err != nil {
					return err
				}
				configuration.Version = item.Version()
				configurations = append(configurations, &configuration)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return configurations, nil
}

func (s *Store) DeleteConfiguration(namespaceID, group, dataID string) error {
	if err := s.db.Update(func(txn *badger.Txn) error {
		configurationKey := newConfigurationKey(namespaceID, group, dataID)
		if err := txn.Delete([]byte(configurationKey)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Store) Snapshot(w io.WriteCloser) error {
	_, err := s.db.Backup(w, 0)
	return err
}
func (s *Store) Restore(r io.ReadCloser) error {
	return s.db.Load(r, 10)
}

// Shutdown close the KV store
func (s *Store) Shutdown() error {
	return s.db.Close()
}
