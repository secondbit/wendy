package pastry

import (
	"encoding/json"
	"errors"
	"io"
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

// Stop gracefully shuts down the local connection to the Cluster, removing the local Node from the Cluster and preventing it from receiving or sending further messages.
//
// Before it disconnects the Node, Stop contacts every Node it knows of to warn them of its departure. If a graceful disconnect is not necessary, Kill should be used instead. Nodes will remove the Node from their state tables next time they attempt to contact it.
func (c *Cluster) Stop() {
	msg := c.NewMessage(NODE_EXIT, NodeID{}, []byte{})
	nodes, err := c.table.export()
	if err != nil {
		c.fanOutError(err)
	}
	for _, node := range nodes {
		err = c.send(msg, node)
		c.fanOutError(err)
	}
	c.Kill()
}

// Kill shuts down the local connection to the Cluster, removing the local Node from the Cluster and preventing it from receiving or sending further messages.
//
// Unlike Stop, Kill immediately disconnects the Node without sending a message to let other Nodes know of its exit.
func (c *Cluster) Kill() {
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
		if _, ok := err.(IdentityError); ok {
			c.deliver(msg)
			return nil
		}
		return err
	}
	if target == nil {
		target, err = c.table.route(msg.Key)
		if err != nil {
			if _, ok := err.(IdentityError); ok {
				c.deliver(msg)
				return nil
			}
			return err
		}
	}
	if target == nil {
		c.deliver(msg)
		return nil
	}
	forward := true
	for _, app := range c.applications {
		f := app.OnForward(&msg, target.ID)
		if forward {
			forward = f
		}
	}
	if forward {
		return c.send(msg, target)
	}
	c.debug("Message %s wasn't forwarded because callback terminated it.", msg.Key)
	return nil
}

// Join announces a Node's presence to the Cluster, kicking off a process that will populate its child leafSet and routingTable. Once that process is complete, the Node can be said to be fully participating in the Cluster.
//
// The IP and port passed to Join should be those of a known Node in the Cluster. The algorithm assumes that the known Node is close in proximity to the current Node, but that is not a hard requirement.
func (c *Cluster) Join(ip string, port int, credentials Credentials) error {
	credentialsJSON, err := json.Marshal(credentials)
	if err != nil {
		return err
	}
	msg := c.NewMessage(NODE_JOIN, c.self.ID, credentialsJSON)
	address := ip + ":" + strconv.Itoa(port)
	return c.sendToIP(msg, address)
}

func (c *Cluster) fanOutError(err error) {
	for _, app := range c.applications {
		app.OnError(err)
	}
}

func (c *Cluster) sendHeartbeats() {
	msg := c.NewMessage(HEARTBEAT, NodeID{}, []byte{})
	nodes, err := c.table.export()
	if err != nil {
		c.fanOutError(err)
	}
	for _, node := range nodes {
		err = c.send(msg, node)
		if err == deadNodeError {
			_, err := c.table.removeNode(node.ID)
			if err != nil && err != nodeNotFoundError {
				if _, ok := err.(IdentityError); !ok {
					c.fanOutError(err)
				}
			}
			continue
		}
	}
}

func (c *Cluster) deliver(msg Message) {
	if msg.Purpose == NODE_JOIN || msg.Purpose == NODE_EXIT || msg.Purpose == HEARTBEAT || msg.Purpose == STAT_DATA || msg.Purpose == STAT_REQ || msg.Purpose == NODE_RACE || msg.Purpose == NODE_REPR {
		c.warn("Received utility message %s to the deliver function. Purpose was %s.", msg.Key, msg.Purpose)
		return
	}
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
	node, err := c.table.getNode(msg.Sender.ID)
	if err == nodeNotFoundError {
		node, err = c.table.insertNode(msg.Sender)
		if err != nil {
			if _, ok := err.(IdentityError); !ok {
				c.fanOutError(err)
			}
		}
	}
	if node != nil {
		node.updateLastHeardFrom()
		node.setProximity(time.Since(msg.Sent).Nanoseconds())
	}
	conn.Write([]byte("Received."))
	conn.Close()
	switch msg.Purpose {
	case NODE_JOIN:
		c.onNodeJoin(msg.Sender)
		break
	case NODE_EXIT:
		c.onNodeExit(msg.Sender)
		break
	case HEARTBEAT:
		for _, app := range c.applications {
			app.OnHeartbeat(msg.Sender)
		}
		break
	case STAT_DATA:
		c.onStateReceived(msg)
		break
	case STAT_REQ:
		c.onStateRequested(msg.Sender)
		break
	case NODE_RACE:
		c.onRaceCondition(msg.Sender)
		break
	case NODE_REPR:
		c.onRepairRequest(msg.Sender)
		break
	default:
		c.onMessageReceived(msg)
	}
}


func (c *Cluster) send(msg Message, destination *Node) error {
	if c.self == nil || destination == nil {
		return errors.New("Can't send to or from a nil node.")
	}
	var address string
	if destination.Region == c.self.Region {
		address = destination.LocalIP + ":" + strconv.Itoa(destination.Port)
	} else {
		address = destination.GlobalIP + ":" + strconv.Itoa(destination.Port)
	}
	err := c.sendToIP(msg, address)
	if err == nil {
		destination.updateLastHeardFrom()
	}
	return err
}

func (c *Cluster) sendToIP(msg Message, address string) error {
	conn, err := net.DialTimeout("tcp", address, time.Duration(c.networkTimeout) * time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(time.Duration(c.networkTimeout) * time.Second))
	conn.SetReadDeadline(time.Now().Add(time.Duration(c.networkTimeout) * time.Second))
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(msg)
	if err != nil {
		return err
	}
	var result []byte
	_, err = conn.Read(result)
	if err != nil {
		if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			return deadNodeError
		}
		if err == io.EOF {
			err = nil
		}
	}
	return err
}

// Our message handlers!

func (c *Cluster) onNodeJoin(node Node) {	
	c.insert(node)
	for _, app := range c.applications {
		app.OnNodeJoin(node)
	}
	c.sendStateTables(node, true, true)
}

func (c *Cluster) onNodeExit(node Node) {
	c.remove(node.ID)
	for _, app := range c.applications {
		app.OnNodeExit(node)
	}
}

// BUG(paddy@secondbit.org): If the Nodes don't agree on the time, Pastry can create an infinite loop of sending state information, detecting an erroneous race condition, and requesting state information. For the love of God, use NTP.
func (c *Cluster) onStateReceived(msg Message) {
	if c.lastStateUpdate.After(msg.Sent) {
		c.warn("Detected race condition. %s sent the message at %s, but we last updated our state tables at %s. Notifying %s.", msg.Sender.ID, msg.Sent, msg.Sender.ID)
		msg := c.NewMessage(NODE_RACE, NodeID{}, []byte{})
		target, err := c.table.getNode(msg.Sender.ID)
		if err != nil {
			if err == nodeNotFoundError {
				c.insert(msg.Sender)
			} else if _, ok := err.(IdentityError); ok {
				c.err("Apparently received state information from myself...?")
				return
			} else {
				c.fanOutError(err)
			}
		}
		if target != nil {
			err = c.send(msg, target)
			if err != nil {
				c.fanOutError(err)
			}
		} else {
			c.err("Node ended up being nil (probably due to errors) when trying to respond to a race condition message.")
		}
		return
	}
	var state []Node
	err := json.Unmarshal(msg.Value, &state)
	if err != nil {
		c.fanOutError(err)
		return
	}
	for _, node := range state {
		c.insert(node)
	}
}

func (c *Cluster) onStateRequested(node Node) {
	c.sendStateTables(node, true, true)
}

func (c *Cluster) onRaceCondition(node Node) {
	msg := c.NewMessage(STAT_REQ, NodeID{}, []byte{})
	target, err := c.table.getNode(node.ID)
	if err != nil {
		if err == nodeNotFoundError {
			c.insert(node)
		} else if _, ok := err.(IdentityError); ok {
			c.warn("Tried to send race condition warning to self.")
			return
		} else {
			c.fanOutError(err)
		}
	}
	if target != nil {
		err = c.send(msg, target)
		if err != nil {
			c.fanOutError(err)
		}
	} else {
		c.err("Node ended up being nil (probably due to errors) when trying to respond to a race condition message.")
	}
}

func (c *Cluster) onRepairRequest(node Node) {
	c.sendStateTables(node, false, true)
}

func (c *Cluster) onMessageReceived(msg Message) {
	err := c.Send(msg)
	if err != nil {
		c.fanOutError(err)
	}
}

func (c *Cluster) sendStateTables(node Node, table, leafset bool) error {
	var state []*Node
	if table {
		dump, err := c.table.export()
		if err != nil {
			return err
		}
		state = append(state, dump...)
	}
	if leafset {
		dump, err := c.table.export()
		if err != nil {
			return err
		}
		state = append(state, dump...)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	msg := c.NewMessage(STAT_DATA, c.self.ID, data)
	target, err := c.table.getNode(node.ID)
	if err != nil {
		if err == nodeNotFoundError {
			c.insert(node)
		} else {
			return err
		}
	}
	return c.send(msg, target)
}

func (c *Cluster) insert(node Node) {
	_, err := c.table.insertNode(node)
	if err != nil {
		if _, ok := err.(IdentityError); !ok {
			c.fanOutError(err)
		}
	}
	resp, err := c.leafset.insertNode(node)
	if err != nil {
		if _, ok := err.(IdentityError); !ok {
			c.fanOutError(err)
		}
	}
	if resp != nil {
		leaves, err := c.leafset.export()
		if err != nil {
			c.fanOutError(err)
		}
		for _, app := range c.applications {
			app.OnNewLeaves(leaves)
		}
	}
	c.lastStateUpdate = time.Now()
}

// BUG(paddy@secondbit.org): If we happen to remove one of our two neighbours in the leaf set, we will wind up with a hole in the leaf set until we next get a state update. The only fix for this that I can think of involves retrieving results by position from the leaf set, and I'm not eager to complicate things with that if I can avoid it.
func (c *Cluster) remove(id NodeID) {
	_, err := c.table.removeNode(id)
	if err != nil && err != nodeNotFoundError {
		if _, ok := err.(IdentityError); !ok {
			c.fanOutError(err)
		}
	}
	resp, err := c.leafset.removeNode(id)
	if err != nil && err != nodeNotFoundError {
		if _, ok := err.(IdentityError); !ok {
			c.fanOutError(err)
		}
	}
	if resp != nil {
		leaves, err := c.leafset.export()
		if err != nil {
			c.fanOutError(err)
		}
		for _, app := range c.applications {
			app.OnNewLeaves(leaves)
		}
		msg := c.NewMessage(NODE_REPR, resp.ID, []byte{})
		err = c.Send(msg)
		if err != nil {
			c.fanOutError(err)
		}
	}
	c.lastStateUpdate = time.Now()
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
