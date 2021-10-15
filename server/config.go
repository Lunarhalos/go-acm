package server

import (
	"fmt"
	"net"
	"time"
)

const (
	DefaultBindAddr = "127.0.0.1:9527"
	DefaultBindPort = 9527
)

type Config struct {
	// NodeName is the name we register as. Defaults to hostname.
	NodeName string `mapstructure:"node-name"`

	// BindAddr is the address on which all of dkron's services will
	// be bound. If not specified, this defaults to the first private ip address.
	BindAddr string `mapstructure:"bind-addr"`

	// HTTPAddr is the address on the UI web server will
	// be bound. If not specified, this defaults to all interfaces.
	HTTPAddr string `mapstructure:"http-addr"`

	// AdvertiseAddr is the address that the Serf and gRPC layer will advertise to
	// other members of the cluster. Can be used for basic NAT traversal
	// where both the internal ip:port and external ip:port are known.
	AdvertiseAddr string `mapstructure:"advertise-addr"`

	// Tags are used to attach key/value metadata to a node.
	Tags map[string]string `mapstructure:"tags"`

	// RPCPort is the gRPC port used by Dkron. This should be reachable
	// by the other servers and clients.
	RPCPort int `mapstructure:"rpc-port"`

	// AdvertiseRPCPort is the gRPC port advertised to clients. This should be reachable
	// by the other servers and clients.
	AdvertiseRPCPort int `mapstructure:"advertise-rpc-port"`

	Profile string `mapstructure:"profile"`

	// DataDir is the directory to store our state in
	DataDir string `mapstructure:"data-dir"`

	ReconnectInterval time.Duration `mapstructure:"reconnect-interval"`
	ReconnectTimeout  time.Duration `mapstructure:"reconnect-timeout"`
	ReconcileInterval time.Duration

	RaftMultiplier int `mapstructure:"raft-multiplier"`
}

func (c *Config) AddrParts(address string) (string, int, error) {
	checkAddr := address

START:
	_, _, err := net.SplitHostPort(checkAddr)
	if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
		checkAddr = fmt.Sprintf("%s:%d", checkAddr, DefaultBindPort)
		goto START
	}
	if err != nil {
		return "", 0, err
	}

	// Get the address
	addr, err := net.ResolveTCPAddr("tcp", checkAddr)
	if err != nil {
		return "", 0, err
	}

	return addr.IP.String(), addr.Port, nil
}

func populateConfig(config *Config) *Config {
	if config == nil {
		config = &Config{}
	}

	if config.NodeName == "" {

	}

	if config.BindAddr == "" {

	}

	if config.HTTPAddr == "" {

	}

	if config.AdvertiseAddr == "" {

	}

	if config.DataDir == "" {

	}
	return config
}
