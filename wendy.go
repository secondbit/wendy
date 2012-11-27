package wendy

import (
	"errors"
	"fmt"
)

type reqMode int

const (
	mode_set = reqMode(iota)
	mode_get
	mode_del
	mode_prx
	mode_scan
	mode_dump
	mode_beat // For getting the nodes that need a heartbeat
)

const (
	LogLevelDebug = iota
	LogLevelWarn
	LogLevelError
)

// Application is an interface that other packages can fulfill to hook into Wendy.
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
	OnForward(msg *Message, nextId NodeID) bool // return False if Wendy should not forward
	OnNewLeaves(leafset []*Node)
	OnNodeJoin(node Node)
	OnNodeExit(node Node)
	OnHeartbeat(node Node)
}

// Credentials is an interface that can be fulfilled to limit access to the Cluster.
type Credentials interface {
	Valid([]byte) bool
	Marshal() []byte
}

// Passphrase is an implementation of Credentials that grants access to the Cluster if the Node has the same Passphrase set
type Passphrase string

func (p Passphrase) Valid(supplied []byte) bool {
	return string(supplied) == string(p)
}

func (p Passphrase) Marshal() []byte {
	return []byte(p)
}

// The below types are used in ensuring concurrency safety within the state tables

type getRequest struct {
	id       NodeID
	strict   bool
	err      chan error
	response chan *Node
}

type dumpRequest struct {
	response chan []*Node
}

type removeRequest struct {
	id       NodeID
	err      chan error
	response chan *Node
}

type insertRequest struct {
	node     *Node
	err      chan error
	tablePos chan routingTablePosition
	leafPos  chan leafSetPosition
}

// Errors!
var deadNodeError = errors.New("Node did not respond to heartbeat.")
var nodeNotFoundError = errors.New("Node not found.")
var impossibleError = errors.New("This error should never be reached. It's logically impossible.")

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

// IdentityError represents an error that was raised when a Node attempted to perform actions on its state tables using its own ID, which is problematic. It is its own type for the purposes of handling the error.
type IdentityError struct {
	Action      string
	Preposition string
	Container   string
}

// Error returns the IdentityError as a string and fulfills the error interface.
func (e IdentityError) Error() string {
	return fmt.Sprintf("IdentityError: Tried to %s myself %s the %s.", e.Action, e.Preposition, e.Container)
}

func throwIdentityError(action, prep, container string) IdentityError {
	return IdentityError{
		Action:      action,
		Preposition: prep,
		Container:   container,
	}
}

// InvalidArgumentError represents an error that is raised when arguments that are invalid are passed to a function that depends on those arguments. It is its own type for the purposes of handling the error.
type InvalidArgumentError string

func (e InvalidArgumentError) Error() string {
	return fmt.Sprintf("InvalidArgumentError: %s", e)
}

func throwInvalidArgumentError(msg string) InvalidArgumentError {
	return InvalidArgumentError(msg)
}
