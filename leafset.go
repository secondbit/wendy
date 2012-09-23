package pastry

import (
	"log"
	"os"
)

type leafSetPosition struct {
	left     bool
	pos      int
	inserted bool
}

type leafSet struct {
	self *Node
	left [16]*Node
	right [16]*Node
	kill chan bool
	log log.Logger
	logLevel int
	timeout int
	request chan interface{}
}

func newLeafSet(self *Node) *leafSet {
	return &leafSet{
		self: self,
		left: [16]*Node{},
		right: [16]*Node{},
		kill: make(chan bool),
		log: log.New(os.Stdout, "pastry#leafSet("+self.ID.String()+")", log.LstdFlags),
		logLevel: LogLevelWarn,
		timeout: 1,
		request: make(chan interface{}),
	}
}

func (l *leafSet) stop() {
	l.kill <- true
}

func (l *leafSet) listen() {
	for {
	loop:
		select {
		case request := <-l.request:
			switch request.(type) {
			case getRequest:
				r := request.(getRequest)
				if r.strict {
					// TODO: get Node
				} else {
					// TODO: scan
				}
				break
			case dumpRequest:
				r := request.(dumpRequest)
				l.dump(r.response)
				break
			case insertRequest:
				r := request.(insertRequest)
				l.insert(r.node, r.leafPos, l.err)
				break
			case removeRequest:
				r := request.(removeRequest)
				// TODO: remove node
				break
			}
			break
		case <-l.kill:
			return
		}
	}
}

func (l *leafSet) insertNode(node Node) (*Node, error) {
	return l.insertValues(node.id, node.localIP, node.globalIP, node.region, node.port)
}

func (l *leafSet) insertValues(id NodeID, localIP, globalIP, region string, port int) (*Node, error) {
	node := NewNode(id, localIP, globalIP, region, port)
	pos := make(chan leafSetPosition)
	err := make(chan error)
	l.request <- insertRequest{
		node: node,
		err: err,
		leafPos: pos,
	}
	select {
	case p := <-pos:
		if p.inserted {
			return nil, nil
		}
		return node, nil
	case e := <-err:
		return nil, e
	case <-time.After(l.timeout * time.Second):
		return nil, throwTimeout("LeafSet insertion", t.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (l *leafSet) insert(node *Node, poschan chan leafSetPosition, errchan chan error) {
	if node == nil {
		// TODO: throw an invalid argument error
	}
	var pos int
	var inserted bool
	side := l.self.ID.RelPos(node.ID)
	if side == -1 {
		l.left, pos, inserted = node.insertIntoArray(l.left, l.self)
		if pos > -1 {
			poschan <- leafSetPosition{
				pos: pos,
				left: true,
				inserted: inserted,
			}
			return
		}
	} else if side == 1 {
		l.right, pos, inserted = node.insertIntoArray(l.right, l.self)
		if pos > -1 {
			poschan <- leafSetPosition{
				pos: pos,
				left: false,
				inserted: inserted,
			}
			return
		}
	}
	// TODO: throw an error
	return
}

func (l *leafSet) export() ([]*Node, error) {
	resp := make(chan []*Node)
	err := make(chan error)
	l.request <- dumpRequest{
		response: resp,
	}
	select {
	case nodes := <-resp:
		return nodes, nil
	case e := <-err:
		return nil, e
	case <-time.After(t.timeout * time.Second):
		return nil, throwTimeout("LeafSet dump", t.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (l *leafSet) dump(resp chan []*Node) {
	nodes := []*Node{}
	for _, node := range l.left {
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	for _, node := range l.right {
		if node != nil {
			nodes = append(nodes, node)
		}
	}
	resp <- nodes
}

func (node *Node) insertIntoArray(array [16]*Node, center *Node) ([16]*Node, int, bool) {
	var result [16]*Node
	result_index := 0
	src_index := 0
	pos := -1
	inserted := false
	for result_index < len(result) {
		result[result_index] = array[src_index]
		if array[src_index] == nil {
			if pos < 0 {
				result[result_index] = node
				pos = result_index
				inserted = true
			}
			break
		}
		if node.ID.Equals(array[src_index].ID) {
			pos = result_index
			continue
		}
		if center.ID.Diff(node.ID).Cmp(center.ID.Diff(result[result_index].ID)) < 0 && pos < 0 {
			result[result_index] = node
			pos = result_index
			inserted = true
		} else {
			src_index += 1
		}
		result_index += 1
	}
	return result, pos, inserted
}
