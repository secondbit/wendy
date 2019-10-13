package wendy

import (
	"errors"
	"log"
	"os"
	"sync"
)

type routingTable struct {
	self     *Node
	nodes    [32][16]*Node
	log      *log.Logger
	logLevel int
	lock     *sync.RWMutex
}

func newRoutingTable(self *Node) *routingTable {
	return &routingTable{
		self:     self,
		nodes:    [32][16]*Node{},
		log:      log.New(os.Stdout, "wendy#routingTable("+self.ID.String()+")", log.LstdFlags),
		logLevel: LogLevelWarn,
		lock:     new(sync.RWMutex),
	}
}

var rtDuplicateInsertError = errors.New("Node already exists in routing table.")

func (t *routingTable) insertNode(node Node, proximity int64) (*Node, error) {
	return t.insertValues(node.ID, node.LocalAddr, node.GlobalAddr, node.Region, node.Port, node.routingTableVersion, node.leafsetVersion, node.neighborhoodSetVersion, proximity)
}

func (t *routingTable) insertValues(id NodeID, LocalAddr, GlobalAddr, region string, port int, rtVersion, lsVersion, nsVersion uint64, proximity int64) (*Node, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	node, err := NewNode(id, LocalAddr, GlobalAddr, region, port)
	if err != nil {
		return nil, err
	}
	node.updateVersions(rtVersion, lsVersion, nsVersion)
	node.setProximity(proximity)
	row := t.self.ID.CommonPrefixLen(node.ID)
	if row >= len(t.nodes) {
		return nil, throwIdentityError("insert", "into", "routing table")
	}
	col := int(node.ID.Digit(row))
	if col >= len(t.nodes[row]) {
		return nil, impossibleError
	}
	if t.nodes[row][col] != nil {
		if node.ID.Equals(t.nodes[row][col].ID) {
			t.debug("Node %s already in routing table. Versions before insert:\nrouting table: %d\nleaf set: %d\nneighborhood set: %d\n", t.nodes[row][col].ID.String(), t.nodes[row][col].routingTableVersion, t.nodes[row][col].leafsetVersion, t.nodes[row][col].neighborhoodSetVersion)
			node.updateVersions(t.nodes[row][col].routingTableVersion, t.nodes[row][col].leafsetVersion, t.nodes[row][col].neighborhoodSetVersion)
			t.nodes[row][col] = node
			t.debug("Versions after insert:\nrouting table: %d\nleaf set: %d\nneighborhood set: %d\n", t.nodes[row][col].routingTableVersion, t.nodes[row][col].leafsetVersion, t.nodes[row][col].neighborhoodSetVersion)
			return nil, rtDuplicateInsertError
		}
		// keep the node that has the closest proximity
		if t.self.Proximity(t.nodes[row][col]) > t.self.Proximity(node) {
			t.nodes[row][col] = node
			t.debug("Inserted node %s into routing table.", node.ID.String())
			return node, nil
		}
	} else {
		t.nodes[row][col] = node
		t.debug("Inserted node %s into routing table.", node.ID.String())
		t.self.incrementRTVersion()
		return node, nil
	}
	return nil, nil
}

func (t *routingTable) getNode(id NodeID) (*Node, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	row := t.self.ID.CommonPrefixLen(id)
	if row >= idLen {
		return nil, throwIdentityError("get", "from", "routing table")
	}
	col := int(id.Digit(row))
	if col >= len(t.nodes[row]) {
		return nil, impossibleError
	}
	if t.nodes[row][col] == nil {
		return nil, nodeNotFoundError
	}
	if !t.nodes[row][col].ID.Equals(id) {
		t.debug("Node not found. Expected %s, got %s.", id.String(), t.nodes[row][col].ID.String())
		return nil, nodeNotFoundError
	}
	return t.nodes[row][col], nil
}

func (t *routingTable) route(id NodeID) (*Node, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	row := t.self.ID.CommonPrefixLen(id)
	if row >= idLen {
		return nil, throwIdentityError("route to", "in", "routing table")
	}
	col := int(id.Digit(row))
	if col >= len(t.nodes[row]) {
		return nil, impossibleError
	}
	if t.nodes[row][col] != nil {
		return t.nodes[row][col], nil
	}
	diff := t.self.ID.Diff(id)
	for scan_row := row; scan_row < len(t.nodes); scan_row++ {
		for c, n := range t.nodes[scan_row] {
			if c == int(t.self.ID.Digit(row)) {
				continue
			}
			if n == nil {
				continue
			}
			entry_diff := n.ID.Diff(id).Cmp(diff)
			if entry_diff == -1 || (entry_diff == 0 && !t.self.ID.Less(n.ID)) {
				return n, nil
			}
		}
	}
	return nil, nodeNotFoundError
}

func (t *routingTable) removeNode(id NodeID) (*Node, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	row := t.self.ID.CommonPrefixLen(id)
	if row >= idLen {
		return nil, throwIdentityError("remove", "from", "routing table")
	}
	col := int(id.Digit(row))
	if col > len(t.nodes[row]) {
		return nil, impossibleError
	}
	if t.nodes[row][col] != nil && t.nodes[row][col].ID.Equals(id) {
		resp := t.nodes[row][col]
		t.nodes[row][col] = nil
		t.self.incrementRTVersion()
		return resp, nil
	} else {
		return nil, nodeNotFoundError
	}
}

func (t *routingTable) list(rows, cols []int) []*Node {
	t.lock.RLock()
	defer t.lock.RUnlock()
	nodes := []*Node{}
	if len(rows) > 0 {
		for _, row := range rows {
			if len(cols) > 0 {
				for _, col := range cols {
					if t.nodes[row][col] != nil {
						nodes = append(nodes, t.nodes[row][col])
					}
				}
			} else {
				for _, col := range t.nodes[row] {
					if col != nil {
						nodes = append(nodes, col)
					}
				}
			}
		}
	} else {
		for _, row := range t.nodes {
			for _, col := range row {
				if col != nil {
					nodes = append(nodes, col)
				}
			}
		}
	}
	return nodes
}

func (t *routingTable) export(rows, cols []int) []state {
	t.lock.RLock()
	defer t.lock.RUnlock()
	states := make([]state, 0)
	if len(rows) > 0 {
		for _, row := range rows {
			if len(cols) > 0 {
				for _, col := range cols {
					if t.nodes[row][col] != nil {
						states = append(states, state{Row: row, Pos: col, Node: *t.nodes[row][col]})
					}
				}
			} else {
				for col, node := range t.nodes[row] {
					if node != nil {
						states = append(states, state{Row: row, Pos: col, Node: *t.nodes[row][col]})
					}
				}
			}
		}
	} else {
		for rowNo, row := range t.nodes {
			for colNo, node := range row {
				if node != nil {
					states = append(states, state{Row: rowNo, Pos: colNo, Node: *node})
				}
			}
		}
	}
	return states
}

func (t *routingTable) debug(format string, v ...interface{}) {
	if t.logLevel <= LogLevelDebug {
		t.log.Printf(format, v...)
	}
}

func (t *routingTable) warn(format string, v ...interface{}) {
	if t.logLevel <= LogLevelWarn {
		t.log.Printf(format, v...)
	}
}

func (t *routingTable) err(format string, v ...interface{}) {
	if t.logLevel <= LogLevelError {
		t.log.Printf(format, v...)
	}
}
