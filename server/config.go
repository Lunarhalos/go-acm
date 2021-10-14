package internal

import (
	"time"
)

type Config struct {
	ID         string
	NameSpace  string
	Name       string
	CreateTime time.Time
	Format     string
	Data       []byte
}
