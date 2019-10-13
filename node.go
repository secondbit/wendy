package wendy

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multiformats/go-multiaddr"
)

// Node represents a specific machine in the cluster.
type Node struct {
	LocalAddr              string // The multiaddr through which the Node should be accessed by other Nodes with an identical Region
	GlobalAddr             string // The multiaddr through which the Node should be accessed by other Nodes whose Region differs
	Port                   int    // The port the Node is listening on
	Region                 string // A string that allows you to intelligently route between local and global requests for, e.g., EC2 regions
	ID                     NodeID
	proximity              int64
	mutex                  *sync.RWMutex // lock and unlock a Node for concurrency safety
	lastHeardFrom          time.Time     // The last time we heard from this node
	leafsetVersion         uint64        // the version number of the leafset
	routingTableVersion    uint64        // the version number of the routing table
	neighborhoodSetVersion uint64        // the version number of the neighborhood set
}

// NewNode initialises a new Node and its associated mutexes. It does *not* update the proximity of the Node.
func NewNode(id NodeID, local, global, region string, port int) (*Node, error) {
	portAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/tcp/%v", port))
	if err != nil {
		return nil, err
	}
	localAddr, err := multiaddr.NewMultiaddr(local)
	if err != nil {
		return nil, err
	}
	globalAddr, err := multiaddr.NewMultiaddr(global)
	if err != nil {
		return nil, err
	}
	return &Node{
		ID:                     id,
		LocalAddr:              localAddr.Encapsulate(portAddr).String(),
		GlobalAddr:             globalAddr.Encapsulate(portAddr).String(),
		Port:                   port,
		Region:                 region,
		proximity:              -1,
		mutex:                  new(sync.RWMutex),
		lastHeardFrom:          time.Now(),
		leafsetVersion:         0,
		routingTableVersion:    0,
		neighborhoodSetVersion: 0,
	}, nil
}

func (self *Node) IPToMultiAaddr(local bool) multiaddr.Multiaddr {
	var addr multiaddr.Multiaddr
	if local {
		addr, _ = multiaddr.NewMultiaddr(self.LocalAddr)
	} else {
		addr, _ = multiaddr.NewMultiaddr(self.GlobalAddr)
	}
	return addr
}

// IsZero returns whether or the given Node has been initialised or if it's an empty Node struct. IsZero returns true if the Node has been initialised, false if it's an empty struct.
func (self Node) IsZero() bool {
	return self.LocalAddr == "" && self.GlobalAddr == "" && self.Port == 0
}

// GetIP returns the multiaddr and port that should be used when communicating with a Node, to respect Regions.
func (self Node) GetIP(other Node) multiaddr.Multiaddr {
	self.mutex.RLock()
	defer self.mutex.RUnlock()
	if other.mutex != nil {
		other.mutex.RLock()
		defer other.mutex.RUnlock()
	}
	var ip multiaddr.Multiaddr
	if self.Region == other.Region {
		ip, _ = multiaddr.NewMultiaddr(other.LocalAddr)
	} else {
		ip, _ = multiaddr.NewMultiaddr(other.GlobalAddr)
	}
	return ip
}

// Proximity returns the proximity score for the Node, adjusted for the Region. The proximity score of a Node reflects how close it is to the current Node; a lower proximity score means a closer Node. Nodes outside the current Region are penalised by a multiplier.
func (self *Node) Proximity(n *Node) int64 {
	if n == nil {
		return -1
	}
	if self.mutex == nil {
		self.mutex = new(sync.RWMutex)
	}
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	multiplier := int64(1)
	if n.Region != self.Region {
		multiplier = 5
	}
	score := n.proximity * multiplier
	return score
}

func (self *Node) getRawProximity() int64 {
	if self.mutex == nil {
		self.mutex = new(sync.RWMutex)
	}
	self.mutex.RLock()
	defer self.mutex.RUnlock()
	return self.proximity
}

func (self *Node) setProximity(proximity int64) {
	if self.mutex == nil {
		self.mutex = new(sync.RWMutex)
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.proximity = proximity
}

func (self *Node) updateLastHeardFrom() {
	if self.mutex == nil {
		self.mutex = new(sync.RWMutex)
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.lastHeardFrom = time.Now()
}

func (self *Node) LastHeardFrom() time.Time {
	if self.mutex == nil {
		self.mutex = new(sync.RWMutex)
	}
	self.mutex.RLock()
	defer self.mutex.RUnlock()
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

func (self *Node) updateVersions(RTVersion, LSVersion, NSVersion uint64) {
	for self.routingTableVersion < RTVersion {
		self.incrementRTVersion()
	}
	for self.leafsetVersion < LSVersion {
		self.incrementLSVersion()
	}
	for self.neighborhoodSetVersion < NSVersion {
		self.incrementNSVersion()
	}
}
