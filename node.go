package pastry

import (
	"errors"
	"strconv"
	"sync"
)

// Node represents a specific machine in the cluster.
type Node struct {
	LocalIP   string // The IP through which the Node should be accessed by other Nodes with an identical Region
	GlobalIP  string // The IP through which the Node should be accessed by other Nodes whose Region differs
	Port      int    // The port the Node is listening on
	Region    string // A string that allows you to intelligently route between local and global requests for, e.g., EC2 regions
	ID        NodeID
	proximity int64       // The raw proximity score for the Node, not adjusted for Region
	mutex     *sync.Mutex // lock and unlock a Node for concurrency safety
}

// NewNode initialises a new Node and its associated mutexes. It does *not* update the proximity of the Node.
func NewNode(id NodeID, local, global, region string, port int) *Node {
	return &Node{
		ID:        id,
		LocalIP:   local,
		GlobalIP:  global,
		Port:      port,
		Region:    region,
		proximity: 0,
		mutex:     new(sync.Mutex),
	}
}

// Proximity returns the proximity score for the Node, adjusted for the Region. The proximity score of a Node reflects how close it is to the current Node; a lower proximity score means a closer Node. Nodes outside the current Region are penalised by a multiplier.
func (self *Node) Proximity(n *Node) int64 {
	n.mutex.Lock()
	if n == nil {
		return -1
	}
	multiplier := int64(1)
	if n.Region != self.Region {
		multiplier = 5
	}
	score := n.proximity * multiplier
	n.mutex.Unlock()
	return score
}

// Send transmits a message from the current Node to the specified Node.
func (self *Node) Send(msg Message, destination *Node) error {
	if self == nil || destination == nil {
		return errors.New("Can't send to or from a nil node.")
	}
	var address string
	if destination.Region == self.Region {
		address = destination.LocalIP + ":" + strconv.Itoa(destination.Port)
	} else {
		address = destination.GlobalIP + ":" + strconv.Itoa(destination.Port)
	}
	return msg.send(address)
}
