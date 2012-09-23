package pastry

import (
	"log"
	"os"
	"time"
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
	log *log.Logger
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
		select {
		case request := <-l.request:
			switch request.(type) {
			case getRequest:
				r := request.(getRequest)
				if r.strict {
					l.get(r.id, r.response, r.err)
				} else {
					l.scan(r.id, r.response, r.err)
				}
				break
			case dumpRequest:
				r := request.(dumpRequest)
				l.dump(r.response)
				break
			case insertRequest:
				r := request.(insertRequest)
				l.insert(r.node, r.leafPos, r.err)
				break
			case removeRequest:
				//r := request.(removeRequest)
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
	return l.insertValues(node.ID, node.LocalIP, node.GlobalIP, node.Region, node.Port)
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
	case <-time.After(time.Duration(l.timeout) * time.Second):
		return nil, throwTimeout("LeafSet insertion", l.timeout)
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

func (l *leafSet) getNode(id NodeID) (*Node, error) {
	resp := make(chan *Node)
	err := make(chan error)
	l.request <- getRequest{
		id: id,
		strict: true,
		err: err,
		response: resp,
	}
	select {
	case node := <-resp:
		return node, nil
	case e:= <-err:
		return nil, e
	case <-time.After(time.Duration(l.timeout) * time.Second):
		return nil, throwTimeout("LeafSet retrieval", l.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (l *leafSet) route(key NodeID) (*Node, error) {
	resp := make(chan *Node)
	err := make(chan error)
	l.request <- getRequest{
		id: key,
		strict: false,
		err: err,
		response: resp,
	}
	select {
	case node := <-resp:
		return node, nil
	case e := <-err:
		return nil, e
	case <-time.After(time.Duration(l.timeout) * time.Second):
		return nil, throwTimeout("LeafSet routing", l.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (l *leafSet) get(id NodeID, resp chan *Node, err chan error) {
	side := l.self.ID.RelPos(id)
	if side == -1 {
		for _, node := range l.left {
			if node == nil || id.Equals(node.ID) {
				resp <- node
				return
			}
		}
	} else if side == 1 {
		for _, node := range l.right {
			if node == nil || id.Equals(node.ID) {
				resp <- node
				return
			}
		}
	}
	// TODO: throw an error
	return
}

func (l *leafSet) scan(key NodeID, resp chan *Node, err chan error) {
	side := l.self.ID.RelPos(key)
	best_score := l.self.ID.Diff(key)
	best := l.self
	if side == -1 {
		for _, node := range l.left {
			diff := key.Diff(node.ID)
			if diff.Cmp(best_score) == -1 || (diff.Cmp(best_score) == 0 && node.ID.Less(best.ID)) {
				best = node
				best_score = diff
			}
		}
	} else {
		for _, node := range l.right {
			diff := key.Diff(node.ID)
			if diff.Cmp(best_score) == -1 || (diff.Cmp(best_score) == 0 && node.ID.Less(best.ID)) {
				best = node
				best_score = diff
			}
		}
	}
	if !best.ID.Equals(l.self.ID) {
		resp <- best
		return
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
	case <-time.After(time.Duration(l.timeout) * time.Second):
		return nil, throwTimeout("LeafSet dump", l.timeout)
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

func (l *leafSet) remove(id NodeID, resp chan *Node, err chan error) {
	side := l.self.ID.RelPos(id)
	pos := -1
	var n *Node
	if side == -1 {
		for index, node := range l.left {
			if node.ID.Equals(id) {
				pos = index
				n = node
				break
			}
		}
	} else {
		for index, node := range l.right {
			if node.ID.Equals(id) {
				pos = index
				n = node
				break
			}
		}
	}
	if pos == -1 || (side == -1 && pos > len(l.left)) || (side == 1 && pos > len(l.right)) || side == 0 {
		// TODO: throw an error
		return
	}
	var slice []*Node
	if side == -1 {
		if len(l.left) == 1 {
			slice = []*Node{}
		} else if pos + 1 == len(l.left) {
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
		if len(l.right) == 1 {
			slice = []*Node{}
		} else if pos + 1 == len(l.right) {
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
	resp <- n
	return
}
