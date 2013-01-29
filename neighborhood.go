package wendy

import (
	"log"
	"os"
	"sync"
)

type neighborhoodSet struct {
	self     *Node
	nodes    [32]*Node
	log      *log.Logger
	logLevel int
	lock     *sync.RWMutex
}

func newNeighborhoodSet(self *Node) *neighborhoodSet {
	return &neighborhoodSet{
		self:     self,
		nodes:    [32]*Node{},
		log:      log.New(os.Stdout, "wendy#neighborhoodSet("+self.ID.String()+")", log.LstdFlags),
		logLevel: LogLevelWarn,
		lock:     new(sync.RWMutex),
	}
}

func (n *neighborhoodSet) insertNode(node Node) (*Node, error) {
	return n.insertValues(node.ID, node.LocalIP, node.GlobalIP, node.Region, node.Port)
}

func (n *neighborhoodSet) insertValues(id NodeID, localIP, globalIP, region string, port int) (*Node, error) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if id.Equals(n.self.ID) {
		return nil, throwIdentityError("insert", "into", "neighborhood set")
	}
	//node := NewNode(id, localIP, globalIP, region, port)
	// TODO: insert node
	n.self.incrementNSVersion()
	return nil, nil
}

func (n *neighborhoodSet) getNode(id NodeID) (*Node, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	if id.Equals(n.self.ID) {
		return nil, throwIdentityError("get", "from", "neighborhood set")
	}
	for _, node := range n.nodes {
		if node == nil {
			break
		}
		if id.Equals(node.ID) {
			return node, nil
		}
	}
	return nil, nodeNotFoundError
}

func (n *neighborhoodSet) export() [16]Node {
	n.lock.RLock()
	defer n.lock.RUnlock()
	nodes := [16]Node{}
	for i, node := range n.nodes {
		if node != nil {
			nodes[i] = *node
		}
	}
	return nodes
}

func (n *neighborhoodSet) list() []*Node {
	n.lock.RLock()
	defer n.lock.RUnlock()
	nodes := []*Node{}
	for _, node := range n.nodes {
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (n *neighborhoodSet) removeNode(id NodeID) (*Node, error) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if id.Equals(n.self.ID) {
		return nil, throwIdentityError("remove", "from", "neighborhood set")
	}
	pos := -1
	var node *Node
	for index, entry := range n.nodes {
		if entry == nil || entry.ID.Equals(id) {
			pos = index
			node = entry
			break
		}
	}
	if pos == -1 || pos > len(n.nodes) {
		return nil, nodeNotFoundError
	}
	var slice []*Node
	if len(n.nodes) == 1 {
		slice = []*Node{}
	} else if pos+1 == len(n.nodes) {
		slice = n.nodes[:pos]
	} else if pos == 0 {
		slice = n.nodes[1:]
	} else {
		slice = append(n.nodes[:pos], n.nodes[pos+1:]...)
	}
	for i, _ := range n.nodes {
		if i < len(slice) {
			n.nodes[i] = slice[i]
		} else {
			n.nodes[i] = nil
		}
	}
	n.self.incrementNSVersion()
	return node, nil
}

func (n *neighborhoodSet) debug(format string, v ...interface{}) {
	if n.logLevel <= LogLevelDebug {
		n.log.Printf(format, v...)
	}
}

func (n *neighborhoodSet) warn(format string, v ...interface{}) {
	if n.logLevel <= LogLevelWarn {
		n.log.Printf(format, v...)
	}
}

func (n *neighborhoodSet) err(format string, v ...interface{}) {
	if n.logLevel <= LogLevelError {
		n.log.Printf(format, v...)
	}
}
