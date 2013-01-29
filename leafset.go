package wendy

import (
	"log"
	"os"
	"sync"
)

type leafSet struct {
	self     *Node
	left     [16]*Node
	right    [16]*Node
	log      *log.Logger
	logLevel int
	lock     *sync.RWMutex
}

func newLeafSet(self *Node) *leafSet {
	return &leafSet{
		self:     self,
		left:     [16]*Node{},
		right:    [16]*Node{},
		log:      log.New(os.Stdout, "wendy#leafSet("+self.ID.String()+")", log.LstdFlags),
		logLevel: LogLevelWarn,
		lock:     new(sync.RWMutex),
	}
}

func (l *leafSet) insertNode(node Node) (*Node, error) {
	return l.insertValues(node.ID, node.LocalIP, node.GlobalIP, node.Region, node.Port)
}

func (l *leafSet) insertValues(id NodeID, localIP, globalIP, region string, port int) (*Node, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	node := NewNode(id, localIP, globalIP, region, port)
	side := l.self.ID.RelPos(node.ID)
	var inserted bool
	if side == -1 {
		l.left, inserted = node.insertIntoArray(l.left, l.self)
		if !inserted {
			return nil, nil
		} else {
			return node, nil
		}
	} else if side == 1 {
		l.right, inserted = node.insertIntoArray(l.right, l.self)
		if !inserted {
			return nil, nil
		} else {
			return node, nil
		}
	}
	return nil, throwIdentityError("insert", "into", "leaf set")
}

func (l *leafSet) getNode(id NodeID) (*Node, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	side := l.self.ID.RelPos(id)
	if side == -1 {
		for _, node := range l.left {
			if node == nil {
				break
			}
			if id.Equals(node.ID) {
				return node, nil
			}
		}
	} else if side == 1 {
		for _, node := range l.right {
			if node == nil {
				break
			}
			if id.Equals(node.ID) {
				return node, nil
			}
		}
	} else {
		return nil, throwIdentityError("get", "from", "leaf set")
	}
	return nil, nodeNotFoundError
}

func (l *leafSet) route(key NodeID) (*Node, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
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
		return nil, nodeNotFoundError
	}
	if !best.ID.Equals(l.self.ID) {
		return best, nil
	} else {
		return nil, throwIdentityError("route to", "in", "leaf set")
	}
	return nil, nodeNotFoundError
}

func (l *leafSet) export() [2][]Node {
	l.lock.RLock()
	defer l.lock.RUnlock()
	nodes := [2][]Node{}
	nodes[0] = []Node{}
	nodes[1] = []Node{}
	for _, node := range l.left {
		if node != nil {
			nodes[0] = append(nodes[0], *node)
		}
	}
	for _, node := range l.right {
		if node != nil {
			nodes[1] = append(nodes[1], *node)
		}
	}
	return nodes
}

func (l *leafSet) list() []*Node {
	l.lock.RLock()
	defer l.lock.RUnlock()
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
	return nodes
}

func (node *Node) insertIntoArray(array [16]*Node, center *Node) ([16]*Node, bool) {
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
	return result, inserted
}

func (l *leafSet) removeNode(id NodeID) (*Node, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	side := l.self.ID.RelPos(id)
	if side == 0 {
		return nil, throwIdentityError("remove", "from", "leaf set")
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
		return nil, nodeNotFoundError
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
	return n, nil
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
