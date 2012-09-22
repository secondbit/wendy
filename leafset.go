package pastry

import (
	"fmt"
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
//The Left field designates whether the Node is to the left or the right of the current Node in the NodeID space. True means to the left, false means to the right.
//
// Methods that return a leafSetRequest will always do their best to fully populate it, meaning the result can be used to, for example, determine the position of a Node.
type leafSetRequest struct {
	Pos        int
	Left       bool
	Node       *Node
	resp       chan *leafSetRequest
	multi_resp chan []*Node
	Mode       reqMode
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

// listen is a low-level helper that will set the LeafSet listening for requests and inserts. Passing a value to the LeafSet's kill property will break the listen loop.
func (l *LeafSet) listen() {
	for {
	loop:
		select {
		case r := <-l.req:
			if r.Node == nil {
				if r.Left && r.Pos >= len(l.left) {
					fmt.Printf("Invalid position: %v, max is %v.\n", r.Pos, len(l.left)-1)
					r.resp <- nil
					break loop
				} else if !r.Left && r.Pos >= len(l.right) {
					fmt.Printf("Invalid position: %v, max is %v.\n", r.Pos, len(l.right)-1)
					r.resp <- nil
					break loop
				}
			}
			switch r.Mode {
			case mode_set:
				r.resp <- l.insert(r)
				break loop
			case mode_get:
				r.resp <- l.get(r)
				break loop
			case mode_del:
				r.resp <- l.remove(r)
				break loop
			case mode_scan:
				r.resp <- l.scan(r)
				break loop
			case mode_dump:
				r.multi_resp <- l.dump()
				break loop
			case mode_beat:
				r.multi_resp <- l.getUnheardFrom()
				break loop
			}
			break loop
		case <-l.kill:
			return
		}
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
	var pos int
	side := l.self.ID.RelPos(r.Node.ID)
	if side == -1 {
		l.left, pos = r.Node.insertIntoArray(l.left, l.self)
		if pos > -1 {
			return &leafSetRequest{Mode: mode_set, Node: r.Node, Pos: pos, Left: true}
		}
	} else if side == 1 {
		l.right, pos = r.Node.insertIntoArray(l.right, l.self)
		if pos > -1 {
			return &leafSetRequest{Mode: mode_set, Node: r.Node, Pos: pos, Left: false}
		}
	}
	return nil
}

// insertIntoArray just inserts the given Node into the array of Nodes such that the nodes will be ordered by ID. It's a helper function for inserting a Node into a LeafSet. It returns an array with the Node inserted and the position of the Node in the new array.
func (node *Node) insertIntoArray(array [16]*Node, center *Node) ([16]*Node, int) {
	var result [16]*Node
	result_index := 0
	src_index := 0
	inserted := -1
	duplicate := false
	for result_index < len(result) {
		result[result_index] = array[src_index]
		if array[src_index] == nil {
			if inserted < 0 {
				result[result_index] = node
				inserted = result_index
			}
			break
		}
		if node.ID.Equals(array[src_index].ID) && inserted < 0 {
			duplicate = true
			continue
		}
		if center.ID.Diff(node.ID).Cmp(center.ID.Diff(result[result_index].ID)) < 0 && inserted < 0 && !duplicate{
			result[result_index] = node
			inserted = result_index
		} else {
			src_index += 1
		}
		result_index += 1
	}
	return result, inserted
}

// Get retrieves a Node from the LeafSet. If no Node (nil) is passed, the pos and left arguments are used to select the Node. If left is true, the counter-clockwise half of the LeafSet is used. Otherwise, the clockwise half is used.
//
// Get returns a populated leafSetRequest object or a TimeoutError. If both returns are nil, the query for a Node returned no results.
//
// Get is a concurrency-safe method, and will return a TimeoutError if the leafSetRequest is blocekd for more than one second.
func (l *LeafSet) Get(node *Node, pos int, left bool) (*leafSetRequest, error) {
	resp := make(chan *leafSetRequest)
	l.req <- &leafSetRequest{Node: node, Pos: pos, Left: left, Mode: mode_get, resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Node retrieval", 1)
	}
	return nil, nil
}

// get does the actual low-level retrieval of a Node from the LeafSet. It should *only* ever be called from the LeafSet's listen method, to preserve its concurrency-safe property.
func (l *LeafSet) get(r *leafSetRequest) *leafSetRequest {
	pos := r.Pos
	left := r.Left
	if r.Node != nil {
		pos = -1
		side := l.self.ID.RelPos(r.Node.ID)
		if side == -1 {
			left = true
		} else if side == 1 {
			left = false
		}
		if left {
			for index, node := range l.left {
				if node == nil || r.Node.ID.Equals(node.ID) {
					pos = index
					break
				}
			}
		} else {
			for index, node := range l.right {
				if node == nil || r.Node.ID.Equals(node.ID) {
					pos = index
					break
				}
			}
		}
	}
	if pos == -1 {
		return nil
	}
	if left && pos >= len(l.left) {
		return nil
	} else if !left && pos >= len(l.right) {
		return nil
	}
	var res *Node
	if left {
		res = l.left[pos]
	} else {
		res = l.right[pos]
	}
	if res == nil {
		return nil
	}
	return &leafSetRequest{Pos: pos, Left: left, Mode: mode_get, Node: res}
}

// Scan retrieves the Node in the LeafSet whose NodeID is closest to the passed NodeID and simultaneously closer than the current Node's NodeID. If there is a tie between to Nodes, the Node with the lower ID is used.
//
// Scan returns a populated leafSetRequest object or a TimeoutError. If both returns are nil, the query for a Node returned no results. Note: Scan should only ever be run *after* running leafset.contains and verifying the message ID falls within the bounds of the leafset. Otherwise, it will *always* return the last node on whichever side the message falls on.
//
// Scan is a concurrency-safe method, and will return a TimeoutError if the leafSetRequest is blocked for more than one second.
func (l *LeafSet) Scan(id NodeID) (*leafSetRequest, error) {
	resp := make(chan *leafSetRequest)
	node := &Node{ID: id}
	l.req <- &leafSetRequest{Node: node, Mode: mode_scan, resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("LeafSet scan", 1)
	}
	return nil, nil
}

// scan does the actual low-level retrieval of a Node from the LeafSet by scanning for the most appropriate Node, such that the Node is more appropriate than the current one. It should *only* ever be called from the LeafSet's listen method, to preserve its concurrency-safe property.
func (l *LeafSet) scan(r *leafSetRequest) *leafSetRequest {
	if r.Node == nil {
		return nil
	}
	side := l.self.ID.RelPos(r.Node.ID)
	best_score := l.self.ID.Diff(r.Node.ID)
	best := l.self
	pos := -1
	if side == -1 {
		for index, node := range l.left {
			diff := r.Node.ID.Diff(node.ID)
			if diff.Cmp(best_score) == -1 || (diff.Cmp(best_score) == 0 && node.ID.Less(best.ID)) {
				best = node
				best_score = diff
				pos = index
			}
		}
	} else {
		for index, node := range l.right {
			if node == nil {
				continue
			}
			diff := r.Node.ID.Diff(node.ID)
			if diff.Cmp(best_score) == -1 || (diff.Cmp(best_score) == 0 && node.ID.Less(best.ID)) {
				best = node
				best_score = diff
				pos = index
			}
		}
	}
	if pos != -1 {
		return &leafSetRequest{Pos: pos, Node: best, Mode: mode_scan, Left: (side == -1)}
	}
	return nil
}

// Remove removes a Node from the LeafSet. If no Node is passed, the pos and left arguments determine which Node to remove.
//
// Remove returns a populated leafSetRequest object or a TimeoutError. If both returns are nil, the Node to be removed was not in the LeafSet at the time of the request.
//
// Remove is a concurrency-safe method, and will return a TimeoutError if it is blocked for more than one second.
func (l *LeafSet) Remove(node *Node, pos int, left bool) (*leafSetRequest, error) {
	resp := make(chan *leafSetRequest)
	l.req <- &leafSetRequest{Pos: pos, Left: left, Node: node, Mode: mode_del, resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Node removal", 1)
	}
	return nil, nil
}

// remove does the actual low-level removal of a Node from the LeafSet. It should *only* ever be called from the LeafSet's listen method, to preserve its concurrency-safe property.
func (l *LeafSet) remove(r *leafSetRequest) *leafSetRequest {
	pos := r.Pos
	left := r.Left
	if r.Node != nil {
		pos = -1
		side := l.self.ID.RelPos(r.Node.ID)
		if side == -1 {
			left = true
		} else if side == 1 {
			left = false
		}
		if left {
			for index, node := range l.left {
				if node.ID.Equals(r.Node.ID) {
					pos = index
					break
				}
			}
		} else {
			for index, node := range l.right {
				if node.ID.Equals(r.Node.ID) {
					pos = index
					break
				}
			}
		}
	}
	if left && pos > len(l.left) {
		return nil
	}
	if !left && pos > len(l.right) {
		return nil
	}
	var n *Node
	if left {
		n = l.left[pos]
		var slice []*Node
		if len(l.left) == 1 {
			slice = []*Node{}
		} else if pos+1 == len(l.left) {
			slice = l.left[:pos]
		} else if pos == 0 {
			slice = l.left[1:]
		} else {
			slice = append(l.left[:pos], l.left[pos+1:]...)
		}
		for i, _ := range l.left {
			if i < len(slice) {
				l.left[i] = slice[i]
			} else {
				l.left[i] = nil
			}
		}
	} else {
		n = l.right[pos]
		var slice []*Node
		if len(l.right) == 1 {
			slice = []*Node{}
		} else if pos+1 == len(l.right) {
			slice = l.right[:pos]
		} else if pos == 0 {
			slice = l.right[1:]
		} else {
			slice = append(l.right[:pos], l.right[pos+1:]...)
		}
		for i, _ := range l.right {
			if i < len(slice) {
				l.right[i] = slice[i]
			} else {
				l.right[i] = nil
			}
		}
	}
	return &leafSetRequest{Node: n, Pos: pos, Left: left, Mode: mode_del}
}

// Dump returns a slice of every Node in the LeafSet.
//
// Dump is a concurrency-safe method, and will return a TimeoutError if it is blocked for more than one second.
func (l *LeafSet) Dump() ([]*Node, error) {
	resp := make(chan []*Node)
	l.req <- &leafSetRequest{Pos: -1, Left: true, Node: nil, Mode: mode_dump, multi_resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Leafset dump", 1)
	}
	return nil, nil
}

// dump is a way to export the contents of the leafset
func (l *LeafSet) dump() []*Node {
	nodes := []*Node{}
	for _, node := range l.left {
		if node == nil {
			break
		}
		nodes = append(nodes, node)
	}
	for _, node := range l.right {
		if node == nil {
			break
		}
		nodes = append(nodes, node)
	}
	return nodes
}

// GetUnheardFrom returns a slice of every Node in the LeafSet that hasn't been heard from in over 5 minutes.
//
// GetUnheardFrom is a concurrency-safe method, and will return a TimeoutError if it is blocked for more than one second.
func (l *LeafSet) GetUnheardFrom() ([]*Node, error) {
	resp := make(chan []*Node)
	l.req <- &leafSetRequest{Pos: -1, Left: true, Node: nil, Mode: mode_beat, multi_resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("getting leaves for heartbeat", 1)
	}
	return nil, nil
}

// getUnheardFrom is a way to export the contents of the leafset
func (l *LeafSet) getUnheardFrom() []*Node {
	nodes := []*Node{}
	cutoff := time.Now().Add(5 * time.Minute * -1)
	for _, node := range l.left {
		if node == nil {
			break
		}
		if node.LastHeardFrom().Before(cutoff) {
			nodes = append(nodes, node)
		}
	}
	for _, node := range l.right {
		if node == nil {
			break
		}
		if node.LastHeardFrom().Before(cutoff) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// route is the logic that handles routing messages within the LeafSet. Messages should never be routed with this method alone. Use the Message.Route method instead.
func (l *LeafSet) route(id NodeID) (*Node, error) {
	if !l.contains(id) {
		return nil, nil
	}
	r, err := l.Scan(id)
	if err != nil {
		return nil, err
	}
	if r != nil {
		if r.Node != nil {
			return r.Node, nil
		}
	}
	return nil, nil
}

// contains checks to see if a NodeID falls within the range covered by a LeafSet.
func (l *LeafSet) contains(id NodeID) bool {
	side := l.self.ID.RelPos(id)
	var extreme *Node
	if side == -1 {
		extreme = lastNode(l.left)
	} else if side == 1 {
		extreme = lastNode(l.right)
	}
	if extreme == nil {
		return false
	}
	if extreme.ID.Less(id) {
		return false
	}
	return true
}

// lastNode returns the last Node in a side of the LeafSet.
func lastNode(side [16]*Node) *Node {
	index := len(side)
	for index > 0 {
		index = index - 1
		if side[index] != nil {
			return side[index]
		}
	}
	return nil
}
