package pastry

import (
	"encoding/json"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

// Cluster holds the information about the state of the network. It is the main interface to the distributed network of Nodes.
type Cluster struct {
	self               *Node
	table              *routingTable
	leafset            *leafSet
	kill               chan bool
	lastStateUpdate    time.Time
	applications       []Application
	log                *log.Logger
	logLevel           int
	heartbeatFrequency int
	networkTimeout     int
}

// ID returns an identifier for the Cluster. It uses the ID of the current Node.
func (c *Cluster) ID() NodeID {
	return c.self.ID
}

// String returns a string representation of the Cluster, in the form of its ID.
func (c *Cluster) String() string {
	return c.ID().String()
}

// SetLogger sets the log.Logger that the Cluster, along with its child routingTable and leafSet, will write to.
func (c *Cluster) SetLogger(l *log.Logger) {
	c.log = l
	c.table.log = l
	c.leafset.log = l
}

// SetLogLevel sets the level of logging that will be written to the Logger. It will be mirrored to the child routingTable and leafSet.
//
// Use pastry.LogLevelDebug to write to the most verbose level of logging, helpful for debugging.
//
// Use pastry.LogLevelWarn (the default) to write on events that may, but do not necessarily, indicate an error.
//
// Use pastry.LogLevelError to write only when an event occurs that is undoubtedly an error.
func (c *Cluster) SetLogLevel(level int) {
	c.logLevel = level
	c.table.logLevel = level
	c.leafset.logLevel = level
}

// SetHeartbeatFrequency sets the frequency in seconds with which heartbeats will be sent from this Node to test the health of other Nodes in the Cluster.
func (c *Cluster) SetHeartbeatFrequency(freq int) {
	c.heartbeatFrequency = freq
}

// SetNetworkTimeout sets the number of seconds before which network requests will be considered timed out and killed.
func (c *Cluster) SetNetworkTimeout(timeout int) {
	c.networkTimeout = timeout
}

// SetChannelTimeouts sets the number of seconds before which channel requests will be considered timed out and return an error for the Cluster's leafSet and routingTable.
func (c *Cluster) SetChannelTimeouts(timeout int) {
	c.table.timeout = timeout
	c.leafset.timeout = timeout
}

// NewCluster creates a new instance of a connection to the network and intialises the state tables and channels it requires.
func NewCluster(self *Node) *Cluster {
	return &Cluster{
		self:               self,
		table:              newRoutingTable(self),
		leafset:            newLeafSet(self),
		kill:               make(chan bool),
		lastStateUpdate:    time.Now(),
		applications:       []Application{},
		log:                log.New(os.Stdout, "pastry("+self.ID.String()+")", log.LstdFlags),
		logLevel:           LogLevelWarn,
		heartbeatFrequency: 300,
		networkTimeout:     10,
	}
}

// Stop shuts down the local connection to the Cluster, removing the local Node from the Cluster and preventing it from receiving or sending further messages.
func (c *Cluster) Stop() {
	c.table.stop()
	c.leafset.stop()
	c.kill <- true
}

// RegisterCallback allows anything that fulfills the Application interface to be hooked into the Pastry's callbacks.
func (c *Cluster) RegisterCallback(app Application) {
	c.applications = append(c.applications, app)
}

// Listen starts the Cluster listening for events, including all the individual listeners for each state sub-object.
//
// Note that Listen does *not* join a Node to the Cluster. The Node must announce its presence before the Node is considered active in the Cluster.
func (c *Cluster) Listen(port int) error {
	portstr := strconv.Itoa(port)
	go c.table.listen()
	go c.leafset.listen()
	ln, err := net.Listen("tcp", ":"+portstr)
	if err != nil {
		c.table.stop()
		c.leafset.stop()
		return err
	}
	defer ln.Close()
	for {
		select {
		case <-c.kill:
			return nil
		case <-time.After(time.Duration(c.heartbeatFrequency) * time.Second):
			go c.sendHeartbeats()
		default:
			conn, err := ln.Accept()
			if err != nil {
				c.fanOutError(err)
				continue
			}
			go c.handleClient(conn)
		}
	}
	return nil
}

// Send routes a message through the Cluster.
func (c *Cluster) Send(msg Message) error {
	target, err := c.leafset.route(msg.Key)
	if err != nil {
		return err
	}
	if target == nil {
		target, err = c.table.route(msg.Key)
		if err != nil {
			return err
		}
	}
	if target == nil {
		c.deliver(msg)
		return nil
	}
	// TODO: Send the message to target
	// TODO: Update the Node's last seen property
	return nil
}

// Join announces a Node's presence to the Cluster, kicking off a process that will populate its child leafSet and routingTable. Once that process is complete, the Node can be said to be fully participating in the Cluster.
//
// The IP and port passed to Join should be those of a known Node in the Cluster. The algorithm assumes that the known Node is close in proximity to the current Node, but that is not a hard requirement.
func (c *Cluster) Join(ip string, port int, credentials Credentials) error {
	// TODO: Build a message announcing the Node's presence
	// TODO: Send the message to the specified IP and port
	return nil
}

func (c *Cluster) fanOutError(err error) {
	for _, app := range c.applications {
		app.OnError(err)
	}
}

func (c *Cluster) sendHeartbeats() {
	// TODO: Build a heartbeat message
	// TODO: Acquire a list of Nodes to send a heartbeat to
	// TODO: Loop through a list of Nodes and send a heartbeat message
	// TODO: Update the Node's last seen property
	// TODO: Remove dead nodes
}

func (c *Cluster) deliver(msg Message) {
	for _, app := range c.applications {
		app.OnDeliver(msg)
	}
}

func (c *Cluster) handleClient(conn net.Conn) {
	defer conn.Close()
	var msg Message
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&msg)
	if err != nil {
		c.fanOutError(err)
		return
	}
	// TODO: Update the Node's last seen property
	// TODO: Update the Node's proximity metric
	// TODO: Switch amongst types of messages, and handle them
}

func (c *Cluster) remove(id NodeID) {
	// TODO: Remove Node from routingTable by ID
	// TODO: Remove Node from leafSet by ID
}

func (c *Cluster) debug(format string, v ...interface{}) {
	if c.logLevel >= LogLevelDebug {
		c.log.Printf(format, v...)
	}
}

func (c *Cluster) warn(format string, v ...interface{}) {
	if c.logLevel >= LogLevelWarn {
		c.log.Printf(format, v...)
	}
}

func (c *Cluster) err(format string, v ...interface{}) {
	if c.logLevel >= LogLevelError {
		c.log.Printf(format, v...)
	}
}
