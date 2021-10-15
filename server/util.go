package server

import (
	"net"
	"strconv"
)

// Get bind address for RPC
func (s *Server) bindRPCAddr() string {
	bindIP, _, _ := s.config.AddrParts(s.config.BindAddr)
	return net.JoinHostPort(bindIP, strconv.Itoa(s.config.RPCPort))
}
