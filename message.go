package pastry

import (
	"encoding/json"
	"errors"
	"net"
	"time"
)

// Message represents the messages that are sent through the cluster of Nodes
type Message struct {
	Purpose byte
	Origin  Node      // The Node a message originated at
	Hops    []NodeID  // A list of the nodes a message went through
	Key     NodeID    // The message's ID
	Value   string    // The message being passed
	Sent    time.Time // The time the message was initially sent
}

const NODE_JOIN = byte(1) // Used when a Node joins the cluster
const NODE_EXIT = byte(2) // Used when a Node leaves the cluster
const NODE_TEST = byte(3) // Used when a Node is being tested
const NODE_STAT = byte(4) // Used when a Node broadcasts state info
const NODE_RACE = byte(5) // Used when a Node hits a race condition
const NODE_REPR = byte(6) // Used when a Node needs to repair its LeafSet

// String returns a string representation of a message.
func (m *Message) String() string {
	return m.Key.String() + ": " + m.Value
}

var deadNodeError = errors.New("Node did not respond to heartbeat.")

// send sends a message to the specified IP address.
func (m *Message) send(ip string) error {
	conn, err := net.Dial("tcp", ip)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(m)
	if err != nil {
		return err
	}
	if m.Purpose == NODE_TEST {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var result []byte
		_, err = conn.Read(result)
		if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			return deadNodeError
		}
	}
	return err
}
