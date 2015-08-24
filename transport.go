package wendy

import (
	"net"
	"time"
)

// Transport represents a low-level network interface
type Transport interface {
	Listen(laddr string) (net.Listener, error)
	DialTimeout(addr string, timeout time.Duration) (net.Conn, error)
}
