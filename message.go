package pastry

import (
	"time"
)

// Message represents the messages that are sent through the cluster of Nodes
type Message struct {
	Purpose byte
	Sender  Node      // The Node a message originated at
	Key     NodeID    // The message's ID
	Value   []byte    // The message being passed
	Sent    time.Time // The time the message was initially sent
}

const NODE_JOIN = byte(0) // Used when a Node joins the cluster
const NODE_EXIT = byte(1) // Used when a Node leaves the cluster
const HEARTBEAT = byte(2) // Used when a Node is being tested
const STAT_SEND = byte(3) // Used when a Node broadcasts state info
const STAT_RECV = byte(4) // Used when a Node is requesting state info
const NODE_RACE = byte(5) // Used when a Node hits a race condition
const NODE_REPR = byte(6) // Used when a Node needs to repair its LeafSet

// String returns a string representation of a message.
func (m *Message) String() string {
	return m.Key.String() + ": " + string(m.Value)
}

func (c *Cluster) NewMessage(purpose byte, key NodeID, value []byte) Message {
	return Message{
		Purpose: purpose,
		Sender: *c.self,
		Key: key,
		Value: value,
		Sent: time.Now(),
	}
}
