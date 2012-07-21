package pastry

import (
	"fmt"
	"sync"
	"time"
)

// TimeoutError represents an error that was raised when a call has taken too long. It is its own type for the purposes of handling the error.
type TimeoutError struct {
	Action  string
	Timeout int
}

// Error returns the TimeoutError as a string and fulfills the error interface.
func (t TimeoutError) Error() string {
	return fmt.Sprintf("Timeout error: %s took more than %d seconds.", t.Action, t.Timeout)
}

// throwTimeout creates a new TimeoutError from the action and timeout specified.
func throwTimeout(action string, timeout int) TimeoutError {
	return TimeoutError{
		Action:  action,
		Timeout: timeout,
	}
}

// routingTableRequest is a request for a specific Node in the routing table. It is simply the row, column, and entry that is to be retrieved, along with the channel that the Node is to be passed to when it has been retrieved.
type routingTableRequest struct {
	row   int
	col   int
	entry int
	resp  chan *Node
}

// Node represents a specific server in the cluster.
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
func (n *Node) Proximity(self *Node) int64 {
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

// RoutingTable is what a Node uses to route requests through the cluster.
// RoutingTables have 32 rows of 16 columns each, and each column has an indeterminate number of entries in it.
// A Node's row in the RoutingTable is the index of the first significant digit between the Node and the Node the RoutingTable belongs to.
// A Node's column in the RoutingTable is the numerical value of the first significant digit between the Node and the Node the RoutingTable belongs to.
// A Node's position in the column is determined by ordering all Nodes in that column by proximity to the Node the RoutingTable belongs to.
//
// RoutingTables are concurrency-safe; the only way to interact with the RoutingTable is through channels.
type RoutingTable struct {
	self  *Node
	nodes [32][16][]*Node
	input chan *Node
	req   chan routingTableRequest
	kill  chan bool
}

// NewRoutingTable initialises a new RoutingTable along with all its corresponding channels.
func NewRoutingTable(self *Node) *RoutingTable {
	nodes := [32][16][]*Node{}
	input := make(chan *Node)
	req := make(chan routingTableRequest)
	kill := make(chan bool)
	return &RoutingTable{
		self:  self,
		nodes: nodes,
		input: input,
		req:   req,
		kill:  kill,
	}
}

// Stops stops a RoutingTable from listening for updates.
func (t *RoutingTable) Stop() {
	t.kill <- true
}

// Insert inserts a new Node into the RoutingTable.
func (t *RoutingTable) Insert(n *Node) {
	t.input <- n
}

// GetNode retrieves a Node from the RoutingTable based on its row, column, and position. The Node is returned, or an error. Note that a nil response from both variables signifies invalid query parameters; either the row, column, or entry was outside the bounds of the table.
//
// GetNode is concurrency-safe, and will return a TimeoutError if it is blocked for more than one second.
func (t *RoutingTable) GetNode(row, col, entry int) (*Node, error) {
	select {
	case n := <-t.getNode(row, col, entry):
		return n, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Node retrieval", 1)
	}
	return nil, nil
}

// getNode is the low-level implementation of Node retrieval. It takes care of the actual retrieval of Nodes, creation of the routingTableRequest, and returns the response channel.
func (t *RoutingTable) getNode(row, col, entry int) chan *Node {
	resp := make(chan *Node)
	t.req <- routingTableRequest{row: row, col: col, entry: entry, resp: resp}
	return resp
}

// listen is a low-level helper that will set the RoutingTable listening for requests and inserts. Passing a value to the RoutingTable's kill property will break the listen loop.
func (t *RoutingTable) listen() {
	for {
	loop:
		select {
		case n := <-t.input:
			row := t.self.ID.CommonPrefixLen(n.ID)
			col := int(n.ID[row].Canonical())
			if t.nodes[row][col] == nil {
				t.nodes[row][col] = []*Node{}
			}
			for _, node := range t.nodes[row][col] {
				if node.ID.Equals(n.ID) {
					break loop
				}
			}
			t.nodes[row][col] = append(t.nodes[row][col], n)
			break loop
		case r := <-t.req:
			if r.row > 31 {
				fmt.Println("Invalid row input: %d", r.row)
				r.resp <- nil
				break loop
			}
			if r.col > 15 {
				fmt.Println("Invalid col input: %d", r.col)
				r.resp <- nil
				break loop
			}
			if r.entry > len(t.nodes[r.row][r.col])-1 {
				fmt.Println("Invalid entry input: %d", r.entry)
				r.resp <- nil
				break loop
			}
			r.resp <- t.nodes[r.row][r.col][r.entry]
			break loop
		case <-t.kill:
			return
		}
	}
}

// Neighborhood contains the 32 closest Nodes to the current Node, based on the amount of time a request takes to complete (with a multiplier for Nodes in a different Region, in an attempt to keep requests within a Region).
//
// The Neighborhood is not used in routing, it is only maintained for ordering entries within columns of the RoutingTable.
type Neighborhood struct {
	nodes [32]*Node
	self  *Node
	input chan *Node
	req   chan neighborhoodRequest
	kill  chan bool
}

// neighborhoodRequest is a request for a specific Node in the Neighborhood. It is simply the position or ID of the Node that is to be retrieved, along with the channel that the Node or position is to be passed to when it has been retrieved.
// If the position is specified, the response will be a Node. If the NodeID is specified, the response will be its position (or -1, if it's not in the Neighborhood).
type neighborhoodRequest struct {
	pos  int
	id   NodeID
	resp chan neighborhoodResponse
}

// neighborhoodResponse is a response from a neighborhoodRequest. It contains either the position or Node that the request resulted in.
type neighborhoodResponse struct {
	pos  int
	node *Node
}

// Contains checks the Neighborhood to see if it contains a NodeID of n and returns a boolean.
//
// Contains is concurrency-safe, and returns a TimeoutError if the check is blocked for longer than 1 second.
func (n *Neighborhood) Contains(node NodeID) (bool, error) {
	select {
	case c := <-n.contains(node):
		if c.pos < 0 {
			return false, nil
		} else {
			return true, nil
		}
	case <-time.After(1 * time.Second):
		return false, throwTimeout("Neighborhood check", 1)
	}
	return false, nil
}

// contains is a low-level function that actually checks the Neighborhood for a NodeID.
// It takes care of the construction of the channels that communicate and the request to the Neighborhood.
func (n *Neighborhood) contains(node NodeID) chan neighborhoodResponse {
	resp := make(chan neighborhoodResponse)
	n.req <- neighborhoodRequest{id: node, pos: -1, resp: resp}
	return resp
}

// listen is a low-level helper that will set the Neighborhood listening for requests and inserts. Passing a value to the Neighborhood's kill property will break the listen loop.
func (n *Neighborhood) listen() {
	for {
	loop:
		select {
		case node := <-n.input:
			fmt.Printf("%s", node.ID)
			loser := -1
			for i, v := range n.nodes {
				if loser < 0 {
					loser = i
					break
				}
				if v.Proximity(n.self) < 0 {
					loser = i
					break
				}
				if v.Proximity(n.self) > n.nodes[loser].Proximity(n.self) {
					loser = i
				}
			}
			n.nodes[loser] = node
			break loop
		case r := <-n.req:
			if r.id != nil {
				for i, v := range n.nodes {
					if v.ID.Equals(r.id) {
						r.resp <- neighborhoodResponse{pos: i, node: v}
						break loop
					}
				}
			} else {
				if r.pos >= 0 && r.pos < 32 {
					r.resp <- neighborhoodResponse{node: n.nodes[r.pos], pos: r.pos}
				} else {
					r.resp <- neighborhoodResponse{node: nil, pos: -1}
				}
			}
			break loop
		case <-n.kill:
			return
		}
	}
}

// LeafSet contains the 32 closest Nodes to the current Node, based on the numerical proximity of their NodeIDs.
//
// The LeafSet is divided into Left and Right; the NodeID space is considered to be circular and thus wraps around. Left contains NodeIDs less than the current NodeID. Right contains NodeIDs greater than the current NodeID.
type LeafSet struct {
	left  [16]*Node
	right [16]*Node
	input chan *Node
	req   chan leafSetRequest
	kill  chan bool
}

// leafSetRequest is a request for a specific Node in the LeafSet. It is simply the position or ID of the Node that is to be retrieved, along with the channel that the Node or ID is to be passed to when it has been retrieved.
// If the position is specified, the response will be a Node. If the ID is specified, the response will be the Node's position, or 0 if it is absent. A negative position indicates it is to the left of the Node. A positive position indicates it is to the right of the Node.
type leafSetRequest struct {
	pos  int
	id   NodeID
	resp chan leafSetResponse
}

// leafSetResponse is a response from a leafSetRequest. It contains either the position or Node that the request resulted in.
type leafSetResponse struct {
	pos  int
	node *Node
}
