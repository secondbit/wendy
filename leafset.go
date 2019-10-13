package wendy

import (
	"errors"
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

var lsDuplicateInsertError = errors.New("Node already exists in leaf set.")

func (l *leafSet) insertNode(node Node) (*Node, error) {
	return l.insertValues(node.ID, node.LocalAddr, node.GlobalAddr, node.Region, node.Port, node.routingTableVersion, node.leafsetVersion, node.neighborhoodSetVersion)
}

func (l *leafSet) insertValues(id NodeID, LocalAddr, GlobalAddr, region string, port int, rTVersion, lSVersion, nSVersion uint64) (*Node, error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	node, err := NewNode(id, LocalAddr, GlobalAddr, region, port)
	if err != nil {
		return nil, err
	}
	node.updateVersions(rTVersion, lSVersion, nSVersion)
	side := l.self.ID.RelPos(node.ID)
	var inserted, contained bool
	if side == -1 {
		l.left, contained, inserted = node.insertIntoArray(l.left, l.self)
		if !contained {
			return nil, nil
		} else if !inserted {
			return nil, lsDuplicateInsertError
		} else {
			l.self.incrementLSVersion()
			return node, nil
		}
	} else if side == 1 {
		l.right, contained, inserted = node.insertIntoArray(l.right, l.self)
		if !contained {
			return nil, nil
		} else if !inserted {
			return nil, lsDuplicateInsertError
		} else {
			l.self.incrementLSVersion()
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

func (l *leafSet) getNextNode(id NodeID) (*Node, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	side := l.self.ID.RelPos(id)
	last := -1
	if side == -1 {
		for pos, node := range l.left {
			if node == nil {
				continue
			} else {
				last = pos
				if node.ID.Less(id) {
					return node, nil
				}
				continue
			}
		}
		if last > -1 {
			return l.left[last], nil
		}
		return nil, nodeNotFoundError
	} else if side == 1 {
		for pos, node := range l.right {
			if node == nil {
				continue
			} else {
				last = pos
				if id.Less(node.ID) {
					return node, nil
				}
				continue
			}
		}
		if last > -1 {
			return l.left[last], nil
		}
		return nil, nodeNotFoundError
	} else {
		return nil, throwIdentityError("get next", "from", "leaf set")
	}
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
}

func (l *leafSet) export() []state {
	l.lock.RLock()
	defer l.lock.RUnlock()
	states := make([]state, 0)
	for pos, node := range l.left {
		if node != nil {
			states = append(states, state{Pos: pos, Side: 0, Node: *node})
		}
	}
	for pos, node := range l.right {
		if node != nil {
			states = append(states, state{Pos: pos, Side: 1, Node: *node})
		}
	}
	return states
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

func (node *Node) insertIntoArray(array [16]*Node, center *Node) ([16]*Node, bool, bool) {
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
			node.updateVersions(array[src_index].routingTableVersion, array[src_index].leafsetVersion, array[src_index].neighborhoodSetVersion)
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
	return result, pos > -1, inserted
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
	l.self.incrementLSVersion()
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
