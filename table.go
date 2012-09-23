package pastry

import (
	"log"
	"os"
	"time"
)

type routingTablePosition struct {
	row      int
	col      int
	entry    int
	inserted bool
}

type routingTable struct {
	self     *Node
	nodes    [32][16][]*Node
	kill     chan bool
	log      *log.Logger
	logLevel int
	timeout  int
	request  chan interface{}
}

func newRoutingTable(self *Node) *routingTable {
	return &routingTable{
		self:     self,
		nodes:    [32][16][]*Node{},
		kill:     make(chan bool),
		log:      log.New(os.Stdout, "pastry#routingTable("+self.ID.String()+")", log.LstdFlags),
		logLevel: LogLevelWarn,
		timeout:  1,
		request:  make(chan interface{}),
	}
}

func (t *routingTable) stop() {
	t.kill <- true
}

func (t *routingTable) listen() {
	for {
		select {
		case request := <-t.request:
			switch request.(type) {
			case getRequest:
				r := request.(getRequest)
				if r.strict {
					t.get(r.id, r.response, r.err)
				} else {
					t.scan(r.id, r.response, r.err)
				}
				break
			case dumpRequest:
				r := request.(dumpRequest)
				t.dump(r.response)
				break
			case insertRequest:
				r := request.(insertRequest)
				t.insert(r.node, r.tablePos, r.err)
				break
			case removeRequest:
				r := request.(removeRequest)
				t.remove(r.id, r.response, r.err)
				break
			}
			break
		case <-t.kill:
			return
		}
	}
}

func (t *routingTable) insertNode(node Node) (*Node, error) {
	return t.insertValues(node.ID, node.LocalIP, node.GlobalIP, node.Region, node.Port)
}

func (t *routingTable) insertValues(id NodeID, localIP, globalIP, region string, port int) (*Node, error) {
	node := NewNode(id, localIP, globalIP, region, port)
	pos := make(chan routingTablePosition)
	err := make(chan error)
	t.request <- insertRequest{
		node:     node,
		err:      err,
		tablePos: pos,
	}
	select {
	case p := <-pos:
		if p.inserted {
			return nil, nil
		}
		return node, nil
	case e := <-err:
		return nil, e
	case <-time.After(time.Duration(t.timeout) * time.Second):
		return nil, throwTimeout("Table insertion", t.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (t *routingTable) insert(node *Node, poschan chan routingTablePosition, errchan chan error) {
	if node == nil {
		// TODO throw an invalid argument error
	}
	row := t.self.ID.CommonPrefixLen(node.ID)
	if row >= len(t.nodes) {
		// TODO throw an error
	}
	col := int(node.ID[row].Canonical())
	if col >= len(t.nodes[row]) {
		// TODO throw an error
	}
	if t.nodes[row][col] == nil {
		t.nodes[row][col] = []*Node{}
	}
	for i, node := range t.nodes[row][col] {
		if node.ID.Equals(node.ID) {
			t.nodes[row][col][i] = node
			poschan <- routingTablePosition{
				row:      row,
				col:      col,
				entry:    i,
				inserted: false,
			}
			return
		}
	}
	t.nodes[row][col] = append(t.nodes[row][col], node)
	poschan <- routingTablePosition{
		row:      row,
		col:      col,
		entry:    len(t.nodes[row][col]) - 1,
		inserted: true,
	}
	return
}

func (t *routingTable) getNode(id NodeID) (*Node, error) {
	resp := make(chan *Node)
	err := make(chan error)
	t.request <- getRequest{
		id:       id,
		strict:   true,
		err:      err,
		response: resp,
	}
	select {
	case node := <-resp:
		return node, nil
	case e := <-err:
		return nil, e
	case <-time.After(time.Duration(t.timeout) * time.Second):
		return nil, throwTimeout("Table retrieval", t.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (t *routingTable) route(key NodeID) (*Node, error) {
	resp := make(chan *Node)
	err := make(chan error)
	t.request <- getRequest{
		id:       key,
		strict:   false,
		err:      err,
		response: resp,
	}
	select {
	case node := <-resp:
		return node, nil
	case e := <-err:
		return nil, e
	case <-time.After(time.Duration(t.timeout) * time.Second):
		return nil, throwTimeout("Table routing", t.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (t *routingTable) get(id NodeID, resp chan *Node, err chan error) {
	row := t.self.ID.CommonPrefixLen(id)
	if row >= len(id) {
		// TODO: Throw an error
		return
	}
	col := int(id[row].Canonical())
	if col >= len(t.nodes[row]) {
		// TODO: Throw an error
		return
	}
	entry := -1
	for i, n := range t.nodes[row][col] {
		if n.ID.Equals(id) {
			entry = i
		}
	}
	if entry < 0 {
		// TODO: Throw an error
		return
	}
	if entry > len(t.nodes[row][col]) {
		// TODO: Throw an error
		return
	}
	resp <- t.nodes[row][col][entry]
	return
}

func (t *routingTable) scan(id NodeID, resp chan *Node, err chan error) {
	var node *Node
	row := t.self.ID.CommonPrefixLen(id)
	if row >= len(id) {
		// TODO: Throw an error
		return
	}
	col := int(id[row].Canonical())
	if col >= len(t.nodes[row]) {
		// TODO: Throw an error
		return
	}
	if len(t.nodes[row][col]) > 0 {
		proximity := int64(-1)
		for _, entry := range t.nodes[row][col] {
			if proximity == -1 || t.self.Proximity(entry) < proximity {
				node = entry
				proximity = t.self.Proximity(entry)
			}
		}
		if proximity == -1 {
			// TODO: Throw an error
		}
		resp <- node
		return
	}
	diff := t.self.ID.Diff(id)
	for scan_row := row; scan_row < len(t.nodes); scan_row++ {
		for c, n := range t.nodes[scan_row] {
			if c == int(t.self.ID[row].Canonical()) {
				continue
			}
			if n == nil || len(n) < 1 {
				continue
			}
			proximity := int64(-1)
			for _, entry := range n {
				if entry == nil {
					continue
				}
				if entry.ID == nil {
					continue
				}
				entry_diff := t.self.ID.Diff(entry.ID).Cmp(diff)
				if entry_diff == -1 || (entry_diff == 00 && !t.self.ID.Less(entry.ID)) {
					if proximity == -1 || proximity < t.self.Proximity(entry) {
						node = entry
						proximity = t.self.Proximity(entry)
					}
				}
			}
			if node != nil {
				if proximity == -1 {
					// TODO: Throw an error
				}
				resp <- node
				return
			}
		}
	}
	// TODO: Throw an error
	return
}

func (t *routingTable) removeNode(id NodeID) (*Node, error) {
	resp := make(chan *Node)
	err := make(chan error)
	t.request <- removeRequest{
		id:       id,
		err:      err,
		response: resp,
	}
	select {
	case node := <-resp:
		return node, nil
	case e := <-err:
		return nil, e
	case <-time.After(time.Duration(t.timeout) * time.Second):
		return nil, throwTimeout("Table removal", t.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (t *routingTable) remove(id NodeID, resp chan *Node, err chan error) {
	var row, col, entry int
	entry = -1
	row = t.self.ID.CommonPrefixLen(id)
	if row >= len(id) {
		// TODO: Throw error
	}
	col = int(id[row].Canonical())
	if col > len(t.nodes[row]) {
		// TODO: Throw error
	}
	for i, n := range t.nodes[row][col] {
		if n.ID.Equals(id) {
			entry = i
		}
	}
	if entry < 0 {
		// TODO: Throw erorr
	}
	resp <- t.nodes[row][col][entry]
	if len(t.nodes[row][col]) == 1 {
		t.nodes[row][col] = []*Node{}
		return
	}
	if len(t.nodes[row][col]) == entry+1 {
		t.nodes[row][col] = t.nodes[row][col][:entry]
		return
	}
	if entry == 0 {
		t.nodes[row][col] = t.nodes[row][col][1:]
		return
	}
	t.nodes[row][col] = append(t.nodes[row][col][:entry], t.nodes[row][col][entry+1:]...)
	return
}

func (t *routingTable) export() ([]*Node, error) {
	resp := make(chan []*Node)
	err := make(chan error)
	t.request <- dumpRequest{
		response: resp,
	}
	select {
	case nodes := <-resp:
		return nodes, nil
	case e := <-err:
		return nil, e
	case <-time.After(time.Duration(t.timeout) * time.Second):
		return nil, throwTimeout("Table dump", t.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (t *routingTable) dump(resp chan []*Node) {
	nodes := []*Node{}
	for _, row := range t.nodes {
		for _, col := range row {
			for _, entry := range col {
				if entry != nil {
					nodes = append(nodes, entry)
				}
			}
		}
	}
	resp <- nodes
}
