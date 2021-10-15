package server

import (
	"fmt"
)

type ConfigEntry struct {
	Namespace string
	Name      string
	Data      []byte
	Version   uint64
}

func (c *ConfigEntry) Validate() error {
	if c.Namespace == "" {
		return fmt.Errorf("namespace cannot be empty")
	}
	if c.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	return nil
}
