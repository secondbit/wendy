package wendy

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

type stateMask byte

const (
	rT = stateMask(1 << iota)
	lS
	nS
	all = rT | lS | nS
)

func (m stateMask) includeRT() bool {
	return m == (m & rT)
}

func (m stateMask) includeLS() bool {
	return m == (m & lS)
}

func (m stateMask) includeNS() bool {
	return m == (m & nS)
}

type stateTables struct {
	RoutingTable    [32][16]Node
	LeafSet         [2][]Node
	NeighborhoodSet [32]Node
}

// Cluster holds the information about the state of the network. It is the main interface to the distributed network of Nodes.
type Cluster struct {
	self               *Node
	table              *routingTable
	leafset            *leafSet
	neighborhoodset    *neighborhoodSet
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

// NewCluster creates a new instance of a connection to the network and intialises the state tables and channels it requires.
func NewCluster(self *Node, credentials Credentials) *Cluster {
	return &Cluster{
		self:               self,
		table:              newRoutingTable(self),
		leafset:            newLeafSet(self),
		neighborhoodset:    newNeighborhoodSet(self),
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
	nodes := c.table.list()
	nodes = append(nodes, c.leafset.list()...)
	nodes = append(nodes, c.neighborhoodset.list()...)
	for _, node := range nodes {
		err := c.send(msg, node)
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
	ln, err := net.Listen("tcp", ":"+portstr)
	if err != nil {
		return err
	}
	defer ln.Close()
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
	target, err := c.leafset.route(msg.Key)
	if err != nil {
		if _, ok := err.(IdentityError); ok {
			c.debug("I'm the target. Delivering message %s", msg.Key)
			c.deliver(msg)
			return nil
		}
		if err != nodeNotFoundError {
			return err
		}
		if target != nil {
			c.debug("Target acquired in leafset.")
		}
	}
	if target == nil {
		c.debug("Target not found in leaf set, checking routing table.")
		target, err = c.table.route(msg.Key)
		if err != nil {
			if _, ok := err.(IdentityError); ok {
				c.deliver(msg)
				return nil
			}
			if err != nodeNotFoundError {
				return err
			}
		}
		c.debug("Target acquired in routing table.")
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

// Join expresses a Node's desire to join the Cluster, kicking off a process that will populate its child leafSet, neighborhoodSet and routingTable. Once that process is complete, the Node can be said to be fully participating in the Cluster.
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
	nodes := c.table.list()
	for _, node := range nodes {
		c.debug("Sending heartbeat to %s", node.ID)
		err := c.send(msg, node)
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
		node, _ := c.table.getNode(msg.Sender.ID)
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
	case NODE_ANN:
		c.onNodeAnnounce(msg)
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

// A node wants to join the cluster. We need to route its message as we normally would, but we should also send it our state tables as appropriate.
func (c *Cluster) onNodeJoin(msg Message) {
	c.debug("\033[4;31mNode %s joined!\033[0m", msg.Key)
	err := c.Send(msg)
	if err != nil {
		c.fanOutError(err)
	}
	// TODO: only send appropriate state tables
	err = c.sendStateTables(msg.Sender, rT|lS)
	if err != nil {
		if err != deadNodeError {
			c.fanOutError(err)
		}
	}
}

// A node has joined the cluster. We need to decide if it belongs in our state tables and if the nodes in the state tables it sends us belong in our state tables. If the version of our state tables it sends to us doesn't match our local version, we need to resend our state tables to prevent a race condition.
func (c *Cluster) onNodeAnnounce(msg Message) {
	c.debug("\0333[4;31mNode %s announced its presence!\033[0m", msg.Key)
	// TODO: update state tables with node and state tables
	// TODO: compare c.self.LeafsetVersion, c.self.RoutingTableVersion, and c.self.NeighborhoodSetVersion against the versions the message uses. If there's a mismatch, send a race condition message with new state tables
}

func (c *Cluster) onNodeExit(node Node) {
	c.debug("Node %s left. :(", node.ID)
	c.remove(node.ID)
}

func (c *Cluster) onStateReceived(msg Message) {
	// TODO: distinguish between routing table, leaf set, and neighborhood set.
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
	// TODO: only send appropriate state tables
	c.sendStateTables(node, rT|lS)
}

func (c *Cluster) onRaceCondition(node Node) {
	c.debug("Race condition. Awkward.")
	// TODO: update state tables with attached state information
	// TODO: re-announce presence to all known nodes
}

func (c *Cluster) onRepairRequest(node Node) {
	c.debug("Helping to repair %s", node.ID)
	// TODO: specialise this on a per-state table basis
	c.sendStateTables(node, lS)
}

func (c *Cluster) onMessageReceived(msg Message) {
	c.debug("Received message %s", msg.Key)
	err := c.Send(msg)
	if err != nil {
		c.fanOutError(err)
	}
}

func (c *Cluster) dumpStateTables(tables stateMask) (stateTables, error) {
	var state stateTables
	if tables.includeRT() {
		state.RoutingTable = c.table.export()
	}
	if tables.includeLS() {
		state.LeafSet = c.leafset.export()
	}
	if tables.includeNS() {
		// TODO: export neighborhood set
	}
	return state, nil
}

func (c *Cluster) sendStateTables(node Node, tables stateMask) error {
	state, err := c.dumpStateTables(tables)
	if err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	msg := c.NewMessage(STAT_DATA, c.self.ID, data)
	target, err := c.table.getNode(node.ID)
	if err != nil {
		if _, ok := err.(IdentityError); !ok && err != nodeNotFoundError {
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
		leaves := c.leafset.list()
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
		leaves := c.leafset.list()
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
