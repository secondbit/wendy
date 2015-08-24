package wendy

import (
	"net"
	"time"
)

// NewTCPTransport returns a new TCP transport
func NewTCPTransport() Transport {
	return &tcpTransport{}
}

type tcpTransport struct {
}

func (t *tcpTransport) Listen(laddr string) (net.Listener, error) {
	return net.Listen("tcp", laddr)
}

func (t *tcpTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}
