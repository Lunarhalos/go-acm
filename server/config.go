package server

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/go-sockaddr/template"
	"github.com/segmentio/ksuid"
	flag "github.com/spf13/pflag"
)

const (
	DefaultListenClientUrls    = "127.0.0.1:9527"
	DefaultAdvertiseClientUrls = "127.0.0.1:9527"
	DefaultListenPeerUrls      = "127.0.0.1:9528"
	DefaultAdvertisePeerUrls   = "127.0.0.1:9528"
	DefaultHTTPAddr            = "0.0.0.0:9001"
	DefaultRetryInterval       = time.Second * 30
)

type Config struct {
	// NodeName is the name we register as. Defaults to hostname.
	NodeName string `mapstructure:"name"`

	ListenClientUrls    string `mapstructure:"listen-client-urls"`
	AdvertiseClientUrls string `mapstructure:"advertise-client-urls"`
	ListenPeerUrls      string `mapstructure:"listen-peer-urls"`
	AdvertisePeerUrls   string `mapstructure:"advertise-peer-urls"`
	// HTTPAddr is the address on the UI web server will
	// be bound. If not specified, this defaults to all interfaces.
	HTTPAddr string `mapstructure:"http-addr"`

	Profile string `mapstructure:"profile"`

	// DataDir is the directory to store our state in
	DataDir      string   `mapstructure:"data-dir"`
	StartJoin    []string `mapstructure:"join"`
	RetryJoinLAN []string `mapstructure:"retry-join"`

	ReconnectInterval time.Duration `mapstructure:"reconnect-interval"`
	ReconnectTimeout  time.Duration `mapstructure:"reconnect-timeout"`
	ReconcileInterval time.Duration

	RaftMultiplier int `mapstructure:"raft-multiplier"`

	LogLevel string `mapstructure:"log-level"`
}

func DefaultConfig() *Config {
	return &Config{
		ListenPeerUrls:      DefaultListenPeerUrls,
		AdvertisePeerUrls:   DefaultAdvertisePeerUrls,
		ListenClientUrls:    DefaultListenClientUrls,
		AdvertiseClientUrls: DefaultAdvertiseClientUrls,
		HTTPAddr:            DefaultHTTPAddr,
		DataDir:             "acm.data",
		Profile:             "lan",
		LogLevel:            "info",
		ReconcileInterval:   60 * time.Second,
		ReconnectTimeout:    24 * time.Hour,
		RaftMultiplier:      1,
	}
}

func ConfigFlagSet() *flag.FlagSet {
	c := DefaultConfig()
	fs := flag.NewFlagSet("server flag set", flag.ContinueOnError)
	fs.String("name", "",
		"Name of this node. Must be unique in the cluster")
	fs.String("listen-peer-urls", c.ListenPeerUrls,
		`Specifies which address the agent should bind to for network services, 
including the internal gossip protocol and RPC mechanism. This should be 
specified in IP format, and can be used to easily bind all network services 
to the same address. The value supports go-sockaddr/template format.
`)
	fs.String("advertise-peer-urls", c.AdvertisePeerUrls,
		`Address used to advertise to other nodes in the cluster. By default,
the bind address is advertised. The value supports 
go-sockaddr/template format.`)
	fs.String("listen-client-urls", c.ListenClientUrls, "")
	fs.String("advertise-client-urls", c.AdvertiseClientUrls, "")
	fs.String("http-addr", c.HTTPAddr,
		`Address to bind the UI web server to. Only used when server. The value 
supports go-sockaddr/template format.`)
	fs.String("profile", c.Profile,
		"Profile is used to control the timing profiles used")
	fs.StringSlice("join", []string{},
		"An initial agent to join with. This flag can be specified multiple times")
	fs.StringSlice("retry-join", []string{},
		`Address of an agent to join at start time with retries enabled. 
Can be specified multiple times.`)
	fs.Int("retry-max", 0,
		`Maximum number of join attempts. Defaults to 0, which will retry indefinitely.`)
	fs.Duration("retry-interval", DefaultRetryInterval,
		"Time to wait between join attempts.")
	fs.String("data-dir", c.DataDir,
		`Specifies the directory to use for server-specific data, including the 
replicated log. By default, this is the top-level data-dir, 
like [/var/lib/acm]`)
	fs.Duration("serf-reconnect-timeout", c.ReconnectTimeout,
		`This is the amount of time to attempt to reconnect to a failed node before 
giving up and considering it completely gone. In Kubernetes, you might need 
this to about 5s, because there is no reason to try reconnects for default 
24h value. Also Raft behaves oddly if node is not reaped and returned with 
same ID, but different IP.
Format there: https://golang.org/pkg/time/#ParseDuration`)
	return fs
}

func addrParts(address string) (string, int, error) {
	checkAddr := address
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
		config.NodeName = ksuid.New().String()
	}
	return config
}

// ParseSingleIPTemplate is used as a helper function to parse out a single IP
// address from a config parameter.
func ParseSingleIPTemplate(ipTmpl string) (string, error) {
	out, err := template.Parse(ipTmpl)
	if err != nil {
		return "", fmt.Errorf("unable to parse address template %q: %v", ipTmpl, err)
	}

	ips := strings.Split(out, " ")
	switch len(ips) {
	case 0:
		return "", errors.New("no addresses found, please configure one")
	case 1:
		return ips[0], nil
	default:
		return "", fmt.Errorf("multiple addresses found (%q), please configure one", out)
	}
}
