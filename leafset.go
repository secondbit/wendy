package pastry

import (
	"time"
)

// LeafSet contains the 32 closest Nodes to the current Node, based on the numerical proximity of their NodeIDs.
//
// The LeafSet is divided into Left and Right; the NodeID space is considered to be circular and thus wraps around. Left contains NodeIDs to the left of the current NodeID. Right contains NodeIDs to the right of the current NodeID.
type LeafSet struct {
	left  [16]*Node
	right [16]*Node
	self  *Node
	req   chan *leafSetRequest
	kill  chan bool
}

// leafSetRequest is a request for a specific Node in the LeafSet. The Node field determines the Node being queried against. Should it not be set, the Pos field is used in its stead, with a negative Pos signifying the Nodes with NodeIDs counter-clockwise from the current Node and a positive Pos signifying the Nodes with NodeIDs clockwise from the current Node.
//
//The Mode field is used to determine whether the request is to insert, get, or remove the Node from the LeafSet.
//
// Methods that return a leafSetRequest will always do their best to fully populate it, meaning the result can be used to, for example, determine the position of a Node.
type leafSetRequest struct {
	Pos  int
	Node *Node
	resp chan *leafSetRequest
	Mode reqMode
}

// NewLeafSet initialises a new LeafSet along with all its corresponding channels.
func NewLeafSet(self *Node) *LeafSet {
	left := [16]*Node{}
	right := [16]*Node{}
	req := make(chan *leafSetRequest)
	kill := make(chan bool)
	return &LeafSet{
		self:  self,
		left:  left,
		right: right,
		req:   req,
		kill:  kill,
	}
}

// Stop stops a LeafSet from listening for requests.
func (l *LeafSet) Stop() {
	l.kill <- true
}

// Insert inserts a new Node into the LeafSet.
//
// Insert will return a populated leafSetRequest or a TimeoutError. If both returns are nil, Insert was unable to store the Node in the LeafSet, as the Node's ID is the same as the current Node's ID or the Node is nil.
//
// Insert is a concurrency-safe method, and will return a TimeoutError if the leafSetRequest is blocked for more than one second.
func (l *LeafSet) Insert(n *Node) (*leafSetRequest, error) {
	resp := make(chan *leafSetRequest)
	l.req <- &leafSetRequest{Node: n, Mode: mode_set, resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Node insertion", 1)
	}
	return nil, nil
}

// insert does the actual low-level insertion of a Node. It should *only* be called from the listen method of the LeafSet, to preserve its concurrency-safe property.
func (l *LeafSet) insert(r *leafSetRequest) *leafSetRequest {
	if r.Node == nil {
		return nil
	}
	side := l.self.ID.RelPos(r.Node.ID)
	if side == -1 {
		for i, n := range(l.left) {
			if n == nil {
				l.left[i] = r.Node
				return &leafSetRequest{Mode: mode_set, Node: r.Node, Pos: (-1 * i + 1)}
			} else {
				rel := n.ID.RelPos(r.Node.ID)
				if rel == -1 {
					// TODO: insert node, push everything left
				}
			}
		}
	} else if side == 1 {
		for i, n := range(l.right) {
			if n == nil {
				l.right[i] = r.Node
				return &leafSetRequest{Mode: mode_set, Node: r.Node, Pos: (i + 1)}
			} else {
				rel := n.ID.RelPos(r.Node.ID)
				if rel == 1 {
					// TODO: insert node, push everything right
				}
			}
		}
	}
	return nil
}
