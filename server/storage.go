package server

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
)

type Storage interface {
	Save(entry *ConfigEntry) error
	Get(namespace, name string) (*ConfigEntry, error)
	Delete(namespace, name string) error
	History(namespace, name string) ([]*ConfigEntry, error)
	Snapshot(w io.WriteCloser) error
	Restore(r io.ReadCloser) error
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

func newLatestKey(namespace, name string) string {
	return fmt.Sprintf("latest_%s_%s", namespace, name)
}

func newHistoryKey(namespace, name string) string {
	return fmt.Sprintf("history_%s_%s", namespace, name)
}

// Save 保存配置条目
func (s *Store) Save(entry *ConfigEntry) error {
	if err := entry.Validate(); err != nil {
		return err
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		var (
			historyEntries []*ConfigEntry
			latestKey      = newLatestKey(entry.Namespace, entry.Name)
			historyKey     = newHistoryKey(entry.Namespace, entry.Name)
		)

		// 查询历史配置
		if item, err := txn.Get([]byte(historyKey)); err != nil && err != badger.ErrNoRewrite {
			return err
		} else {
			if err := item.Value(func(val []byte) error {
				if err := json.Unmarshal(val, &historyEntries); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		}

		// 查询当前配置
		if item, err := txn.Get([]byte(latestKey)); err != nil && err != badger.ErrNoRewrite {
			return err
		} else {
			if err := item.Value(func(val []byte) error {
				// 把当前配置加入到历史记录
				historyEntries = append(historyEntries, &ConfigEntry{
					Namespace: entry.Namespace,
					Name:      entry.Name,
					Data:      val,
					Version:   item.Version(),
				})
				return nil
			}); err != nil {
				return err
			}
		}
		if len(historyEntries) > 0 {
			historyValue, err := json.Marshal(&historyEntries)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(historyKey), historyValue); err != nil {
				return err
			}
		}
		if err := txn.Set([]byte(latestKey), entry.Data); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Get(namespace, name string) (*ConfigEntry, error) {
	var latest *ConfigEntry
	err := s.db.View(func(txn *badger.Txn) error {
		latestKey := newLatestKey(namespace, name)
		item, err := txn.Get([]byte(latestKey))
		if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		latest = &ConfigEntry{
			Namespace: namespace,
			Name:      name,
			Data:      data,
			Version:   item.Version(),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return latest, nil
}

func (s *Store) History(namespace, name string) ([]*ConfigEntry, error) {
	var history []*ConfigEntry
	err := s.db.View(func(txn *badger.Txn) error {
		historyKey := newHistoryKey(namespace, name)
		item, err := txn.Get([]byte(historyKey))
		if err != nil {
			return err
		}
		if err := item.Value(func(val []byte) error {
			if err := json.Unmarshal(val, &history); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return history, nil
}

func (s *Store) Delete(namespace, name string) error {
	if err := s.db.Update(func(txn *badger.Txn) error {
		latestKey := newLatestKey(namespace, name)
		historyKey := newHistoryKey(namespace, name)
		if err := txn.Delete([]byte(latestKey)); err != nil {
			return err
		}
		if err := txn.Delete([]byte(historyKey)); err != nil {
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
