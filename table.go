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
	return fmt.Sprintf("Timeout error: %s took more than %v seconds.", t.Action, t.Timeout)
}

// throwTimeout creates a new TimeoutError from the action and timeout specified.
func throwTimeout(action string, timeout int) TimeoutError {
	return TimeoutError{
		Action:  action,
		Timeout: timeout,
	}
}

type reqMode int

const SET = reqMode(0)
const GET = reqMode(1)
const DEL = reqMode(2)
const PRX = reqMode(3)

// routingTableRequest is a request for a specific Node in the routing table. The Node field determines the Node being queried against. Should it not be set, the Row/Col/Entry fields are used in its stead.
//
//The Mode field is used to determine whether the request is to insert, get, or remove the Node from the RoutingTable.
//
// Methods that return a routingTableRequest will always do their best to fully populate it, meaning the result can be used to, for example, determine the Row/Col/Entry of a Node.
type routingTableRequest struct {
	Row   int
	Col   int
	Entry int
	Mode  reqMode
	Node  *Node
	resp  chan *routingTableRequest
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

// RoutingTable is what a Node uses to route requests through the cluster.
// RoutingTables have 32 rows of 16 columns each, and each column has an indeterminate number of entries in it.
//
// A Node's row in the RoutingTable is the index of the first significant digit between the Node and the Node the RoutingTable belongs to.
//
// A Node's column in the RoutingTable is the numerical value of the first significant digit between the Node and the Node the RoutingTable belongs to.
//
// Nodes are simply appended to the end of the slice that each column contains, so their position in the slice has no bearing on routing. The Node.Proximity() method should be used in that case.
//
// RoutingTables are concurrency-safe; the only way to interact with the RoutingTable is through channels.
type RoutingTable struct {
	self  *Node
	nodes [32][16][]*Node
	input chan *Node
	req   chan *routingTableRequest
	kill  chan bool
}

// NewRoutingTable initialises a new RoutingTable along with all its corresponding channels.
func NewRoutingTable(self *Node) *RoutingTable {
	nodes := [32][16][]*Node{}
	input := make(chan *Node)
	req := make(chan *routingTableRequest)
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
//
// Insert will return a populated routingTableRequest or a TimeoutError. If both returns are nil, Insert was unable to store the Node in the RoutingTable, as the Node's ID is the same as the current Node's ID or the Node is nil.
//
// Insert is a concurrency-safe method, and will return a TimeoutError if the routingTableRequest is blocked for more than one second.
func (t *RoutingTable) Insert(n *Node) (*routingTableRequest, error) {
	resp := make(chan *routingTableRequest)
	t.req <- &routingTableRequest{Node: n, Mode: SET, resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Node insertion", 1)
	}
	return nil, nil
}

// insert does the actual low-level insertion of a Node. It should *only* be called from the listen method of the RoutingTable, to preserve its concurrency-safe property.
func (t *RoutingTable) insert(r *routingTableRequest) *routingTableRequest {
	if r.Node == nil {
		return nil
	}
	row := t.self.ID.CommonPrefixLen(r.Node.ID)
	if row > len(t.nodes) {
		return nil
	}
	col := int(r.Node.ID[row].Canonical())
	if col > len(t.nodes[row]) {
		return nil
	}
	if t.nodes[row][col] == nil {
		t.nodes[row][col] = []*Node{}
	}
	for i, node := range t.nodes[row][col] {
		if node.ID.Equals(r.Node.ID) {
			t.nodes[row][col][i] = node
			return &routingTableRequest{Mode: SET, Node: node, Row: row, Col: col, Entry: i}
		}
	}
	t.nodes[row][col] = append(t.nodes[row][col], r.Node)
	return &routingTableRequest{Mode: SET, Node: r.Node, Row: row, Col: col, Entry: len(t.nodes[row][col]) - 1}
}

// Get retrieves a Node from the RoutingTable. If no Node (nil) is passed, the row, col, and entry arguments are used to select the node.
//
// Get returns a populated routingTableRequest object or a TimeoutError. If both returns are nil, the query for a Node returned no results.
//
// Get is a concurrency-safe method, and will return a TimeoutError if the routingTableRequest is blocekd for more than one second.
func (t *RoutingTable) Get(node *Node, row, col, entry int) (*routingTableRequest, error) {
	resp := make(chan *routingTableRequest)
	t.req <- &routingTableRequest{Node: node, Row: row, Col: col, Entry: entry, Mode: GET, resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Node retrieval", 1)
	}
	return nil, nil
}

// get does the actual low-level retrieval of a Node from the RoutingTable. It should *only* ever be called from the RoutingTable's listen method, to preserve its concurrency-safe property.
func (t *RoutingTable) get(r *routingTableRequest) *routingTableRequest {
	row := r.Row
	col := r.Col
	entry := r.Entry
	if r.Node != nil {
		entry = -1
		row = t.self.ID.CommonPrefixLen(r.Node.ID)
		if row > len(r.Node.ID) {
			return nil
		}
		col = int(r.Node.ID[row].Canonical())
		if col > len(t.nodes[row]) {
			return nil
		}
		for i, n := range t.nodes[row][col] {
			if n.ID.Equals(r.Node.ID) {
				entry = i
			}
		}
		if entry < 0 {
			return nil
		}
	}
	if row >= len(t.nodes) {
		return nil
	}
	if col >= len(t.nodes[row]) {
		return nil
	}
	if entry >= len(t.nodes[row][col]) {
		return nil
	}
	return &routingTableRequest{Row: row, Col: col, Entry: entry, Mode: GET, Node: t.nodes[row][col][entry]}
}

// GetByProximity retrieves a Node from the RoutingTable based on its proximity score. Nodes will be ordered by their Proximity scores, lowest first, before selecting the entry from the specified row and column.
//
// GetByProximity returns a populated routingTableRequest object or a TimeoutError. If both returns are nil, the query for a Node returned no results.
//
// GetByProximity is a concurrency-safe method, and will return a TimeoutError if the routingTableRequest is blocekd for more than one second.
func (t *RoutingTable) GetByProximity(row, col, entry int) (*routingTableRequest, error) {
	resp := make(chan *routingTableRequest)
	t.req <- &routingTableRequest{Node: nil, Row: row, Col: col, Entry: entry, Mode: PRX, resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Node retrieval by proximity", 1)
	}
	return nil, nil
}

// proximity does the actual low-level retrieval of a Node from the RoutingTable based on its proximity. It should *only* ever be called from the RoutingTable's listen method, to preserve its concurrency-safe property.
func (t *RoutingTable) proximity(r *routingTableRequest) *routingTableRequest {
	if r.Row >= len(t.nodes) {
		return nil
	}
	if r.Col >= len(t.nodes[r.Row]) {
		return nil
	}
	if r.Entry > len(t.nodes[r.Row][r.Col]) {
		return nil
	}
	entry := -1
	proximity := int64(-1)
	prev_prox := int64(-1)
	for x := 0; x <= r.Entry; x++ {
		entry = -1
		for i, n := range t.nodes[r.Row][r.Col] {
			if entry == -1 {
				entry = i
				proximity = t.self.Proximity(n)
				continue
			}
			p := t.self.Proximity(n)
			if p < proximity && p >= prev_prox {
				entry = i
				prev_prox = proximity
				proximity = p
			}
		}
	}
	return &routingTableRequest{Row: r.Row, Col: r.Col, Entry: entry, Mode: PRX, Node: t.nodes[r.Row][r.Col][entry]}
}

// Remove removes a Node from the RoutingTable. If no Node is passed, the row, column, and position arguments determine which Node to remove.
//
// Remove returns a populated routingTableRequest object or a TimeoutError. If both returns are nil, the Node to be removed was not in the RoutingTable at the time of the request.
//
// Remove is a concurrency-safe method, and will return a TimeoutError if it is blocked for more than one second.
func (t *RoutingTable) Remove(node *Node, row, col, entry int) (*routingTableRequest, error) {
	resp := make(chan *routingTableRequest)
	t.req <- &routingTableRequest{Row: row, Col: col, Entry: entry, Node: node, Mode: DEL, resp: resp}
	select {
	case r := <-resp:
		return r, nil
	case <-time.After(1 * time.Second):
		return nil, throwTimeout("Node removal", 1)
	}
	return nil, nil
}

// remove does the actual low-level removal of a Node from the RoutingTable. It should *only* ever be called from the RoutingTable's listen method, to preserve its concurrency-safe property.
func (t *RoutingTable) remove(r *routingTableRequest) *routingTableRequest {
	row := r.Row
	col := r.Col
	entry := r.Entry
	if r.Node != nil {
		entry = -1
		row = t.self.ID.CommonPrefixLen(r.Node.ID)
		if row > len(r.Node.ID) {
			return nil
		}
		col = int(r.Node.ID[row].Canonical())
		if col > len(t.nodes[row]) {
			return nil
		}
		for i, n := range t.nodes[row][col] {
			if n.ID.Equals(r.Node.ID) {
				entry = i
			}
		}
		if entry < 0 {
			return nil
		}
	}
	if len(t.nodes[row][col]) < entry+1 {
		return nil
	}
	if len(t.nodes[row][col]) == 1 {
		resp := &routingTableRequest{Node: t.nodes[row][col][0], Row: row, Col: col, Entry: 0, Mode: DEL}
		t.nodes[row][col] = []*Node{}
		return resp
	}
	if entry+1 == len(t.nodes[row][col]) {
		resp := &routingTableRequest{Node: t.nodes[row][col][entry], Row: row, Col: col, Entry: entry, Mode: DEL}
		t.nodes[row][col] = t.nodes[row][col][:entry]
		return resp
	}
	if entry == 0 {
		resp := &routingTableRequest{Node: t.nodes[row][col][entry], Row: row, Col: col, Entry: entry, Mode: DEL}
		t.nodes[row][col] = t.nodes[row][col][1:]
		return resp
	}
	resp := &routingTableRequest{Node: t.nodes[row][col][entry], Row: row, Col: col, Entry: entry, Mode: DEL}
	t.nodes[row][col] = append(t.nodes[row][col][:entry], t.nodes[row][col][entry+1:]...)
	return resp
}

// listen is a low-level helper that will set the RoutingTable listening for requests and inserts. Passing a value to the RoutingTable's kill property will break the listen loop.
func (t *RoutingTable) listen() {
	for {
	loop:
		select {
		case r := <-t.req:
			if r.Node == nil {
				if r.Row >= len(t.nodes) {
					fmt.Printf("Invalid row input: %v, max is %v.\n", r.Row, len(t.nodes)-1)
					r.resp <- nil
					break loop
				}
				if r.Col >= len(t.nodes[r.Row]) {
					fmt.Printf("Invalid col input: %v, max is %v.\n", r.Col, len(t.nodes[r.Row])-1)
					r.resp <- nil
					break loop
				}
				if r.Entry >= len(t.nodes[r.Row][r.Col]) {
					fmt.Printf("Invalid entry input: %v, max is %v.\n", r.Entry, len(t.nodes[r.Row][r.Col])-1)
					r.resp <- nil
					break loop
				}
			}
			switch r.Mode {
			case SET:
				r.resp <- t.insert(r)
				break loop
			case GET:
				r.resp <- t.get(r)
				break loop
			case DEL:
				r.resp <- t.remove(r)
				break loop
			case PRX:
				r.resp <- t.proximity(r)
				break loop
			}
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
