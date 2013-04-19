package wendy

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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
	credentials        Credentials
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
// Use wendy.LogLevelDebug to write to the most verbose level of logging, helpful for debugging.
//
// Use wendy.LogLevelWarn (the default) to write on events that may, but do not necessarily, indicate an error.
//
// Use wendy.LogLevelError to write only when an event occurs that is undoubtedly an error.
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
func NewCluster(self *Node, credentials Credentials) *Cluster {
	return &Cluster{
		self:               self,
		table:              newRoutingTable(self),
		leafset:            newLeafSet(self),
		kill:               make(chan bool),
		lastStateUpdate:    time.Now(),
		applications:       []Application{},
		log:                log.New(os.Stdout, "wendy("+self.ID.String()+") ", log.LstdFlags),
		logLevel:           LogLevelWarn,
		heartbeatFrequency: 300,
		networkTimeout:     10,
		credentials:        credentials,
	}
}

// Stop gracefully shuts down the local connection to the Cluster, removing the local Node from the Cluster and preventing it from receiving or sending further messages.
//
// Before it disconnects the Node, Stop contacts every Node it knows of to warn them of its departure. If a graceful disconnect is not necessary, Kill should be used instead. Nodes will remove the Node from their state tables next time they attempt to contact it.
func (c *Cluster) Stop() {
	c.debug("Sending graceful exit message.")
	msg := c.NewMessage(NODE_EXIT, c.self.ID, []byte{})
	nodes, err := c.table.export()
	if err != nil {
		c.fanOutError(err)
	}
	for _, node := range nodes {
		err = c.send(msg, node)
		if err != nil {
			c.fanOutError(err)
		}
	}
	c.Kill()
}

// Kill shuts down the local connection to the Cluster, removing the local Node from the Cluster and preventing it from receiving or sending further messages.
//
// Unlike Stop, Kill immediately disconnects the Node without sending a message to let other Nodes know of its exit.
func (c *Cluster) Kill() {
	c.debug("Exiting the cluster.")
	c.table.stop()
	c.leafset.stop()
	c.kill <- true
}

// RegisterCallback allows anything that fulfills the Application interface to be hooked into the Wendy's callbacks.
func (c *Cluster) RegisterCallback(app Application) {
	c.applications = append(c.applications, app)
}

// Listen starts the Cluster listening for events, including all the individual listeners for each state sub-object.
//
// Note that Listen does *not* join a Node to the Cluster. The Node must announce its presence before the Node is considered active in the Cluster.
func (c *Cluster) Listen() error {
	portstr := strconv.Itoa(c.self.Port)
	c.debug("Listening on port %d", c.self.Port)
	go c.table.listen()
	go c.leafset.listen()
	ln, err := net.Listen("tcp", ":"+portstr)
	if err != nil {
		c.table.stop()
		c.leafset.stop()
		return err
	}
	defer ln.Close()
	// save bound port back to Node in case where port is autoconfigured by OS
	if c.self.Port == 0 {
		addr_info := strings.Split(ln.Addr().String(), ":")
		port, err := strconv.ParseInt(addr_info[len(addr_info)-1], 10, 32)
		if err != nil {
			c.log.Println("Couldn't record autoconfigured port:", err)
		}
		c.self.Port = int(port)
	}
	connections := make(chan net.Conn)
	go func(ln net.Listener, ch chan net.Conn) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				c.fanOutError(err)
				return
			}
			c.debug("Connection received.")
			ch <- conn
		}
	}(ln, connections)
	for {
		select {
		case <-c.kill:
			return nil
		case <-time.After(time.Duration(c.heartbeatFrequency) * time.Second):
			c.debug("Sending heartbeats.")
			go c.sendHeartbeats()
			break
		case conn := <-connections:
			c.debug("Handling connection.")
			c.handleClient(conn)
			break
		}
	}
	return nil
}

// Send routes a message through the Cluster.
func (c *Cluster) Send(msg Message) error {
	c.debug("Getting target for message %s", msg.Key)
	target, err := c.Route(msg.Key)
	if err != nil {
		return err
	}
	if target == nil {
		c.debug("Couldn't find a target. Delivering message %s", msg.Key)
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
		err = c.send(msg, target)
		if err == deadNodeError {
			c.remove(target.ID)
		}
	}
	c.debug("Message %s wasn't forwarded because callback terminated it.", msg.Key)
	return nil
}

// Route checks the leafSet and routingTable to see if there's an appropriate match for the NodeID. If there is a better match than the current Node, a pointer to that Node is returned. Otherwise, nil is returned (and the message should be delivered).
func (c *Cluster) Route(key NodeID) (*Node, error) {
	target, err := c.leafset.route(key)
	if err != nil {
		if _, ok := err.(IdentityError); ok {
			c.debug("I'm the target. Delivering message %s", key)
			return nil, nil
		}
		if err != nodeNotFoundError {
			return nil, err
		}
		if target != nil {
			c.debug("Target acquired in leafset.")
			return target, nil
		}
	}
	c.debug("Target not found in leaf set, checking routing table.")
	target, err = c.table.route(key)
	if err != nil {
		if _, ok := err.(IdentityError); ok {
			c.debug("I'm the target. Delivering message %s", key)
			return nil, nil
		}
		if err != nodeNotFoundError {
			return nil, err
		}
	}
	if target != nil {
		c.debug("Target acquired in routing table.")
		return target, nil
	}
	return nil, nil
}

// Join announces a Node's presence to the Cluster, kicking off a process that will populate its child leafSet and routingTable. Once that process is complete, the Node can be said to be fully participating in the Cluster.
//
// The IP and port passed to Join should be those of a known Node in the Cluster. The algorithm assumes that the known Node is close in proximity to the current Node, but that is not a hard requirement.
func (c *Cluster) Join(ip string, port int) error {
	credentials := c.credentials.Marshal()
	c.debug("Sending join message to %s:%d", ip, port)
	msg := c.NewMessage(NODE_JOIN, c.self.ID, credentials)
	address := ip + ":" + strconv.Itoa(port)
	return c.sendToIP(msg, address)
}

func (c *Cluster) fanOutError(err error) {
	c.err(err.Error())
	for _, app := range c.applications {
		app.OnError(err)
	}
}

func (c *Cluster) sendHeartbeats() {
	msg := c.NewMessage(HEARTBEAT, c.self.ID, []byte{})
	nodes, err := c.table.export()
	if err != nil {
		c.fanOutError(err)
	}
	for _, node := range nodes {
		c.debug("Sending heartbeat to %s", node.ID)
		err = c.send(msg, node)
		if err == deadNodeError {
			c.remove(node.ID)
			continue
		}
	}
}

func (c *Cluster) deliver(msg Message) {
	if msg.Purpose == NODE_JOIN || msg.Purpose == NODE_EXIT || msg.Purpose == HEARTBEAT || msg.Purpose == STAT_DATA || msg.Purpose == STAT_REQ || msg.Purpose == NODE_RACE || msg.Purpose == NODE_REPR {
		c.warn("Received utility message %s to the deliver function. Purpose was %d.", msg.Key, msg.Purpose)
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
	valid := c.credentials == nil
	if !valid {
		valid = c.credentials.Valid(msg.Credentials)
	}
	if !valid {
		c.warn("Credentials did not match. Supplied credentials: %s", msg.Credentials)
		return
	}
	if msg.Purpose != NODE_JOIN {
		node, err := c.table.getNode(msg.Sender.ID)
		if err == nodeNotFoundError {
			_, node = c.insert(msg.Sender)
		}
		if node != nil {
			node.updateLastHeardFrom()
			node.setProximity(time.Since(msg.Sent).Nanoseconds())
		}
	}
	conn.Write([]byte(`{"status": "Received."}`))
	c.debug("Got message with purpose %v", msg.Purpose)
	switch msg.Purpose {
	case NODE_JOIN:
		c.onNodeJoin(msg)
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
	c.debug("Sending message %s to %s", msg.Key, address)
	err := c.sendToIP(msg, address)
	if err == nil {
		destination.updateLastHeardFrom()
	}
	return err
}

func (c *Cluster) sendToIP(msg Message, address string) error {
	c.debug("Sending message %s", string(msg.Value))
	conn, err := net.DialTimeout("tcp", address, time.Duration(c.networkTimeout)*time.Second)
	if err != nil {
		c.debug(err.Error())
		return deadNodeError
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Duration(c.networkTimeout) * time.Second))
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(msg)
	if err != nil {
		return err
	}
	c.debug("Sent message %s to %s", msg.Key, address)
	_, err = conn.Read(nil)
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

func (c *Cluster) onNodeJoin(msg Message) {
	c.debug("\033[4;31mNode %s joined!\033[0m", msg.Key)
	err := c.Send(msg)
	if err != nil {
		c.fanOutError(err)
	}
	c.insert(msg.Sender)
	err = c.sendStateTables(msg.Sender, true, true)
	if err != nil {
		if err == deadNodeError {
			c.remove(msg.Sender.ID)
		} else {
			c.fanOutError(err)
		}
	}
}

func (c *Cluster) onNodeExit(node Node) {
	c.debug("Node %s left. :(", node.ID)
	c.remove(node.ID)
}

// BUG(paddy@secondbit.org): If the Nodes don't agree on the time, Wendy can create an infinite loop of sending state information, detecting an erroneous race condition, and requesting state information. For the love of God, use NTP.
func (c *Cluster) onStateReceived(msg Message) {
	c.debug("Got state information!")
	if c.lastStateUpdate.After(msg.Sent) {
		c.warn("Detected race condition. %s sent the message at %s, but we last updated our state tables at %s. Notifying %s.", msg.Sender.ID, msg.Sent, c.lastStateUpdate, msg.Sender.ID)
		message := c.NewMessage(NODE_RACE, msg.Sender.ID, []byte{})
		target, err := c.table.getNode(msg.Sender.ID)
		if err != nil {
			if err == nodeNotFoundError {
				target, _ = c.insert(msg.Sender)
			} else if _, ok := err.(IdentityError); ok {
				c.warn("Apparently received state information from myself...?")
				return
			} else {
				c.fanOutError(err)
			}
		}
		if target != nil {
			err = c.send(message, target)
			if err != nil {
				c.fanOutError(err)
			}
		} else {
			c.warn("Node ended up being nil (probably due to errors) when trying to respond to a race condition message.")
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
		c.debug("Inserting %s", node.ID)
		c.insert(node)
	}
	c.debug("Inserting %s", msg.Sender.ID)
	c.insert(msg.Sender)
}

func (c *Cluster) onStateRequested(node Node) {
	c.debug("%s wants to know about my state tables!", node.ID)
	c.sendStateTables(node, true, true)
}

func (c *Cluster) onRaceCondition(node Node) {
	c.debug("Race condition. Awkward.")
	c.sendStateTables(node, true, true)
}

func (c *Cluster) onRepairRequest(node Node) {
	c.debug("Helping to repair %s", node.ID)
	c.sendStateTables(node, false, true)
}

func (c *Cluster) onMessageReceived(msg Message) {
	c.debug("Received message %s", msg.Key)
	err := c.Send(msg)
	if err != nil {
		c.fanOutError(err)
	}
	c.insert(msg.Sender)
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
		} else if _, ok := err.(IdentityError); !ok {
			return err
		}
	}
	c.debug("Sending state tables to %s", node.ID)
	return c.send(msg, target)
}

func (c *Cluster) insert(node Node) (leafset, rt *Node) {
	resp, err := c.table.insertNode(node)
	if err != nil {
		if _, ok := err.(IdentityError); !ok {
			c.fanOutError(err)
		}
	}
	if resp != nil {
		c.debug("Inserted node %s into the routing table.", node.ID)
	}
	c.debug("Trying to insert %s into the leaf set.", node.ID)
	resp2, err := c.leafset.insertNode(node)
	if err != nil {
		if _, ok := err.(IdentityError); !ok {
			c.fanOutError(err)
		}
	}
	c.debug("Finished trying to insert %s into the leaf set.", node.ID)
	if resp2 != nil {
		c.debug("Inserted node %s into the leaf set.", node.ID)
		leaves, err := c.leafset.export()
		if err != nil {
			c.fanOutError(err)
		}
		for _, app := range c.applications {
			app.OnNewLeaves(leaves)
		}
	}
	if resp != nil || resp2 != nil {
		for _, app := range c.applications {
			app.OnNodeJoin(node)
		}
	}
	c.lastStateUpdate = time.Now()
	return resp, resp2
}

// BUG(paddy@secondbit.org): If we happen to remove one of our two neighbours in the leaf set, we will wind up with a hole in the leaf set until we next get a state update. The only fix for this that I can think of involves retrieving results by position from the leaf set, and I'm not eager to complicate things with that if I can avoid it.
func (c *Cluster) remove(id NodeID) {
	resp, err := c.table.removeNode(id)
	if err != nil && err != nodeNotFoundError {
		if _, ok := err.(IdentityError); !ok {
			c.fanOutError(err)
		}
	}
	resp2, err := c.leafset.removeNode(id)
	if err != nil && err != nodeNotFoundError {
		if _, ok := err.(IdentityError); !ok {
			c.fanOutError(err)
		}
	}
	if resp2 != nil {
		leaves, err := c.leafset.export()
		if err != nil {
			c.fanOutError(err)
		}
		for _, app := range c.applications {
			app.OnNewLeaves(leaves)
		}
		msg := c.NewMessage(NODE_REPR, resp2.ID, []byte{})
		err = c.Send(msg)
		if err != nil {
			c.fanOutError(err)
		}
	}
	if resp != nil {
		for _, app := range c.applications {
			app.OnNodeExit(*resp)
		}
	} else if resp2 != nil {
		for _, app := range c.applications {
			app.OnNodeExit(*resp2)
		}
	}
	c.lastStateUpdate = time.Now()
}

func (c *Cluster) debug(format string, v ...interface{}) {
	if c.logLevel <= LogLevelDebug {
		c.log.Printf(format, v...)
	}
}

func (c *Cluster) warn(format string, v ...interface{}) {
	if c.logLevel <= LogLevelWarn {
		c.log.Printf(format, v...)
	}
}

func (c *Cluster) err(format string, v ...interface{}) {
	if c.logLevel <= LogLevelError {
		c.log.Printf(format, v...)
	}
}
