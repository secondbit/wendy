package wendy

import (
	"sync"
	"sync/atomic"
	"time"
)

// Node represents a specific machine in the cluster.
type Node struct {
	LocalIP                string // The IP through which the Node should be accessed by other Nodes with an identical Region
	GlobalIP               string // The IP through which the Node should be accessed by other Nodes whose Region differs
	Port                   int    // The port the Node is listening on
	Region                 string // A string that allows you to intelligently route between local and global requests for, e.g., EC2 regions
	ID                     NodeID
	proximity              int64
	mutex                  *sync.Mutex // lock and unlock a Node for concurrency safety
	lastHeardFrom          time.Time   // The last time we heard from this node
	leafsetVersion         uint64      // the version number of the leafset
	routingTableVersion    uint64      // the version number of the routing table
	neighborhoodSetVersion uint64      // the version number of the neighborhood set
}

// NewNode initialises a new Node and its associated mutexes. It does *not* update the proximity of the Node.
func NewNode(id NodeID, local, global, region string, port int) *Node {
	return &Node{
		ID:                     id,
		LocalIP:                local,
		GlobalIP:               global,
		Port:                   port,
		Region:                 region,
		proximity:              -1,
		mutex:                  new(sync.Mutex),
		lastHeardFrom:          time.Now(),
		leafsetVersion:         0,
		routingTableVersion:    0,
		neighborhoodSetVersion: 0,
	}
}

// IsZero returns whether or the given Node has been initialised or if it's an empty Node struct. IsZero returns true if the Node has been initialised, false if it's an empty struct.
func (self Node) IsZero() bool {
	return self.LocalIP == "" && self.GlobalIP == "" && self.Port == 0
}

// Proximity returns the proximity score for the Node, adjusted for the Region. The proximity score of a Node reflects how close it is to the current Node; a lower proximity score means a closer Node. Nodes outside the current Region are penalised by a multiplier.
func (self *Node) Proximity(n *Node) int64 {
	if n == nil {
		return -1
	}
	n.mutex.Lock()
	defer n.mutex.Unlock()
	multiplier := int64(1)
	if n.Region != self.Region {
		multiplier = 5
	}
	score := n.proximity * multiplier
	return score
}

func (self *Node) setProximity(proximity int64) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.proximity = proximity
}

func (self *Node) updateLastHeardFrom() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.lastHeardFrom = time.Now()
}

func (self *Node) LastHeardFrom() time.Time {
	self.mutex.Lock()
	self.mutex.Unlock()
	return self.lastHeardFrom
}

func (self *Node) incrementLSVersion() {
	atomic.AddUint64(&self.leafsetVersion, 1)
}

func (self *Node) incrementRTVersion() {
	atomic.AddUint64(&self.routingTableVersion, 1)
}

func (self *Node) incrementNSVersion() {
	atomic.AddUint64(&self.neighborhoodSetVersion, 1)
}
