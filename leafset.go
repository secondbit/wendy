package wendy

import (
	"log"
	"os"
	"runtime"
	"time"
)

type leafSetPosition struct {
	left     bool
	pos      int
	inserted bool
}

type leafSet struct {
	self     *Node
	left     [16]*Node
	right    [16]*Node
	kill     chan bool
	log      *log.Logger
	logLevel int
	timeout  int
	request  chan interface{}
}

func newLeafSet(self *Node) *leafSet {
	return &leafSet{
		self:     self,
		left:     [16]*Node{},
		right:    [16]*Node{},
		kill:     make(chan bool),
		log:      log.New(os.Stdout, "wendy#leafSet("+self.ID.String()+")", log.LstdFlags),
		logLevel: LogLevelWarn,
		timeout:  1,
		request:  make(chan interface{}),
	}
}

func (l *leafSet) stop() {
	l.kill <- true
}

func (l *leafSet) listen() {
	for {
		runtime.Gosched()
		select {
		case request, ok := <-l.request:
			if !ok {
				panic("Listen channel closed?")
			}
			switch r := request.(type) {
			case getRequest:
				if r.strict {
					l.get(r.id, r.response, r.err)
				} else {
					l.scan(r.id, r.response, r.err)
				}
				break
			case dumpRequest:
				l.dump(r.response)
				break
			case insertRequest:
				l.debug("Insert request routed.")
				l.insert(r.node, r.leafPos, r.err)
				break
			case removeRequest:
				l.remove(r.id, r.response, r.err)
				break
			}
			break
		case _, ok := <-l.kill:
			if !ok {
				panic("kill channel closed?")
			}
			return
		}
	}
}

func (l *leafSet) insertNode(node Node) (*Node, error) {
	return l.insertValues(node.ID, node.LocalIP, node.GlobalIP, node.Region, node.Port)
}

func (l *leafSet) insertValues(id NodeID, localIP, globalIP, region string, port int) (*Node, error) {
	l.debug("Insert request received.")
	node := NewNode(id, localIP, globalIP, region, port)
	pos := make(chan leafSetPosition)
	err := make(chan error)
	l.request <- insertRequest{
		node:    node,
		err:     err,
		leafPos: pos,
	}
	l.debug("Request sent.")
	select {
	case p := <-pos:
		l.debug("Response received.")
		if !p.inserted {
			return nil, nil
		}
		return node, nil
	case e := <-err:
		l.debug("Error received.")
		return nil, e
	case <-time.After(time.Duration(l.timeout) * time.Second):
		l.debug("Timeout received.")
		return nil, throwTimeout("LeafSet insertion", l.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (l *leafSet) insert(node *Node, poschan chan leafSetPosition, errchan chan error) {
	l.debug("Inserting...")
	if node == nil {
		l.debug("Node is nil. Throwing invalid argument error.")
		errchan <- throwInvalidArgumentError("Can't insert a nil Node into the leaf set.")
		return
	}
	var pos int
	var inserted bool
	side := l.self.ID.RelPos(node.ID)
	if side == -1 {
		l.debug("Node goes on the left.")
		l.left, pos, inserted = node.insertIntoArray(l.left, l.self)
		if pos > -1 {
			l.debug("Replying to request...")
			poschan <- leafSetPosition{
				pos:      pos,
				left:     true,
				inserted: inserted,
			}
			l.debug("Replied to request.")
			return
		}
	} else if side == 1 {
		l.debug("Node goes on the right.")
		l.right, pos, inserted = node.insertIntoArray(l.right, l.self)
		if pos > -1 {
			l.debug("Replying to request...")
			poschan <- leafSetPosition{
				pos:      pos,
				left:     false,
				inserted: inserted,
			}
			l.debug("Replied to request")
			return
		}
	}
	l.debug("Oops, tried to insert myself in my own leafset. Throwing IdentityError.")
	errchan <- throwIdentityError("insert", "into", "leaf set")
	return
}

func (l *leafSet) getNode(id NodeID) (*Node, error) {
	resp := make(chan *Node)
	err := make(chan error)
	l.request <- getRequest{
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
			if node == nil {
				break
			}
			if id.Equals(node.ID) {
				resp <- node
				return
			}
		}
	} else if side == 1 {
		for _, node := range l.right {
			if node == nil {
				break
			}
			if id.Equals(node.ID) {
				resp <- node
				return
			}
		}
	} else {
		err <- throwIdentityError("get", "from", "leaf set")
		return
	}
	err <- nodeNotFoundError
	return
}

func (l *leafSet) scan(key NodeID, resp chan *Node, err chan error) {
	defer close(resp)
	defer close(err)
	side := l.self.ID.RelPos(key)
	best_score := l.self.ID.Diff(key)
	best := l.self
	biggest := l.self.ID
	if side == -1 {
		for _, node := range l.left {
			if node == nil {
				break
			}
			diff := key.Diff(node.ID)
			if diff.Cmp(best_score) == -1 || (diff.Cmp(best_score) == 0 && node.ID.Less(best.ID)) {
				best = node
				best_score = diff
			}
			biggest = node.ID
		}
	} else {
		for _, node := range l.right {
			if node == nil {
				break
			}
			diff := key.Diff(node.ID)
			if diff.Cmp(best_score) == -1 || (diff.Cmp(best_score) == 0 && node.ID.Less(best.ID)) {
				best = node
				best_score = diff
			}
			biggest = node.ID
		}
	}
	if biggest.Less(key) {
		err <- nodeNotFoundError
		return
	}
	if !best.ID.Equals(l.self.ID) {
		resp <- best
		return
	} else {
		err <- throwIdentityError("route to", "in", "leaf set")
		return
	}
	err <- nodeNotFoundError
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
			result_index += 1
			src_index += 1
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

func (l *leafSet) removeNode(id NodeID) (*Node, error) {
	resp := make(chan *Node)
	err := make(chan error)
	l.request <- removeRequest{
		id:       id,
		err:      err,
		response: resp,
	}
	select {
	case node := <-resp:
		return node, nil
	case e := <-err:
		return nil, e
	case <-time.After(time.Duration(l.timeout) * time.Second):
		return nil, throwTimeout("LeafSet removal", l.timeout)
	}
	panic("Should not be reached")
	return nil, nil
}

func (l *leafSet) remove(id NodeID, resp chan *Node, err chan error) {
	side := l.self.ID.RelPos(id)
	if side == 0 {
		err <- throwIdentityError("remove", "from", "leaf set")
		return
	}
	pos := -1
	var n *Node
	if side == -1 {
		for index, node := range l.left {
			if node == nil || node.ID.Equals(id) {
				pos = index
				n = node
				break
			}
		}
	} else {
		for index, node := range l.right {
			if node == nil || node.ID.Equals(id) {
				pos = index
				n = node
				break
			}
		}
	}
	if pos == -1 || (side == -1 && pos > len(l.left)) || (side == 1 && pos > len(l.right)) {
		err <- nodeNotFoundError
		return
	}
	var slice []*Node
	if side == -1 {
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
	resp <- n
	return
}

func (l *leafSet) debug(format string, v ...interface{}) {
	if l.logLevel <= LogLevelDebug {
		l.log.Printf(format, v...)
	}
}

func (l *leafSet) warn(format string, v ...interface{}) {
	if l.logLevel <= LogLevelWarn {
		l.log.Printf(format, v...)
	}
}

func (l *leafSet) err(format string, v ...interface{}) {
	if l.logLevel <= LogLevelError {
		l.log.Printf(format, v...)
	}
}
