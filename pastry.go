package pastry

import (
	"fmt"
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

const mode_set = reqMode(0)
const mode_get = reqMode(1)
const mode_del = reqMode(2)
const mode_prx = reqMode(3)
const mode_scan = reqMode(4)
const mode_dump = reqMode(5)
const mode_beat = reqMode(6) // For getting the nodes that need a heartbeat

const LogLevelDebug = 2
const LogLevelWarn = 1
const LogLevelError = 0

// Application is an interface that other packages can fulfill to hook into Pastry.
//
// OnError is called on errors that are even remotely recoverable, passing the error that was raised.
//
// OnDeliver is called when the current Node is determined to be the final destination of a Message. It passes the Message that was received.
//
// OnForward is called immediately before a Message is forwarded to the next Node in its route through the Cluster. The function receives a pointer to the Message, which can be modified before it is sent, and the ID of the next step in the Message's route. The function must return a boolean; true if the Message should continue its way through the Cluster, false if the Message should be prematurely terminated instead of forwarded.
//
// OnNewLeaves is called when the current Node's leafSet is updated. The function receives a dump of the leafSet.
//
// OnNodeJoin is called when the current Node learns of a new Node in the Cluster. It receives the Node that just joined.
//
// OnNodeExit is called when a Node is discovered to no longer be participating in the Cluster. It is passed the Node that just left the Cluster. Note that by the time this method is called, the Node is no longer reachable.
//
// OnHeartbeat is called when the current Node receives a heartbeat from another Node. Heartbeats are sent at a configurable interval, if no messages have been sent between the Nodes, and serve the purpose of a health check.
type Application interface {
	OnError(err error)
	OnDeliver(msg Message)
	OnForward(msg *Message, nextId NodeID) bool // return False if Pastry should not forward
	OnNewLeaves(leafset []*Node)
	OnNodeJoin(node Node)
	OnNodeExit(node Node)
	OnHeartbeat(node Node)
}

// Credentials is an interface that can be fulfilled to limit access to the Cluster. It only requires that the values are capable of being marshalled to and from JSON and that they have a Valid method that accepts another set of Credentials and returns true if they are valid (e.g., match), and false if they are invalid.
type Credentials interface {
	MarshalJSON() ([]byte, error)
	UnmarshalJSON([]byte) error
	Valid(Credentials) bool
}

// The below types are used in ensuring concurrency safety within the state tables

type getRequest struct {
	id     NodeID
	strict bool
	err    chan error
	response chan *Node
}

type dumpRequest struct {
	response chan []*Node
}

type removeRequest struct {
	id NodeID
	err chan error
	response chan *Node
}

type insertRequest struct {
	node *Node
	err chan error
	tablePos chan routingTablePosition
	leafPos chan leafSetPosition
}
