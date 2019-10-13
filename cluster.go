package wendy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

type StateMask struct {
	Mask byte
	Rows []int
	Cols []int
}

const (
	rT = byte(1 << iota)
	lS
	nS
	all = rT | lS | nS
)

func (m StateMask) includeRT() bool {
	return m.Mask == (m.Mask | rT)
}

func (m StateMask) includeLS() bool {
	return m.Mask == (m.Mask | lS)
}

func (m StateMask) includeNS() bool {
	return m.Mask == (m.Mask | nS)
}

type state struct {
	Row  int  `json:"row,omitempty"`
	Pos  int  `json:"pos,omitempty"`
	Side int  `json:"side,omitempty"`
	Node Node `json:"node"`
}

type stateTables struct {
	RoutingTable    []state `json:"rt,omitempty"`
	LeafSet         []state `json:"ls,omitempty"`
	NeighborhoodSet []state `json:"ns,omitempty"`
	EOL             bool    `json:"eol,omitempty"`
}

type proximityCache struct {
	cache  map[NodeID]int64
	ticker <-chan time.Time
	*sync.RWMutex
}

func newProximityCache() *proximityCache {
	return &proximityCache{
		cache:   map[NodeID]int64{},
		ticker:  time.Tick(1 * time.Hour),
		RWMutex: new(sync.RWMutex),
	}
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
	joined             bool
	lock               *sync.RWMutex
	proximityCache     *proximityCache
}

func (c *Cluster) newLeaves(leaves []*Node) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	c.debug("Sending newLeaves notifications.")
	for i, app := range c.applications {
		app.OnNewLeaves(leaves)
		c.debug("Sent newLeaves notification %d of %d.", i+1, len(c.applications))
	}
	c.debug("Sent newLeaves notifications.")
}

func (c *Cluster) fanOutJoin(node Node) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, app := range c.applications {
		c.debug("Announcing node join.")
		app.OnNodeJoin(node)
		c.debug("Announced node join.")
	}
}

func (c *Cluster) forward(msg Message, id NodeID) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	forward := true
	for _, app := range c.applications {
		f := app.OnForward(&msg, id)
		if forward {
			forward = f
		}
	}
	return forward
}

func (c *Cluster) marshalCredentials() []byte {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.credentials == nil {
		return []byte{}
	}
	return c.credentials.Marshal()
}

func (c *Cluster) getNetworkTimeout() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.networkTimeout
}

func (c *Cluster) cacheProximity(id NodeID, proximity int64) {
	c.proximityCache.Lock()
	defer c.proximityCache.Unlock()
	c.proximityCache.cache[id] = proximity
}

func (c *Cluster) getCachedProximity(id NodeID) int64 {
	c.proximityCache.RLock()
	defer c.proximityCache.RUnlock()
	if proximity, set := c.proximityCache.cache[id]; set {
		return proximity
	}
	return -1
}

func (c *Cluster) clearProximityCache() {
	c.proximityCache.Lock()
	defer c.proximityCache.Unlock()
	c.proximityCache.cache = map[NodeID]int64{}
}

func (c *Cluster) isJoined() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.joined
}

// ID returns an identifier for the Cluster. It uses the ID of the current Node.
func (c *Cluster) ID() NodeID {
	return c.self.ID
}

// String returns a string representation of the Cluster, in the form of its ID.
func (c *Cluster) String() string {
	return c.ID().String()
}

// GetIP returns the multi address to use when communicating with a Node.
func (c *Cluster) GetIP(node Node) multiaddr.Multiaddr {
	return c.self.GetIP(node)
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
		joined:             false,
		lock:               new(sync.RWMutex),
		proximityCache:     newProximityCache(),
	}
}

// Stop gracefully shuts down the local connection to the Cluster, removing the local Node from the Cluster and preventing it from receiving or sending further messages.
//
// Before it disconnects the Node, Stop contacts every Node it knows of to warn them of its departure. If a graceful disconnect is not necessary, Kill should be used instead. Nodes will remove the Node from their state tables next time they attempt to contact it.
func (c *Cluster) Stop() {
	c.debug("Sending graceful exit message.")
	msg := c.NewMessage(NODE_EXIT, c.self.ID, []byte{})
	nodes := c.table.list([]int{}, []int{})
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
	c.lock.Lock()
	defer c.lock.Unlock()
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
	// save bound port back to Node in case where port is autoconfigured by OS
	if c.self.Port == 0 {
		c.debug("Port set to 0")
		colonPos := strings.LastIndex(ln.Addr().String(), ":")
		if colonPos == -1 {
			c.debug("OS returned an address without a port.")
			return errors.New("OS returned an address without a port.")
		}
		port, err := strconv.ParseInt(ln.Addr().String()[colonPos+1:], 10, 32)
		if err != nil {
			c.debug("Couldn't record autoconfigured port: %s", err.Error())
			return errors.New("Couldn't record autoconfigured port: " + err.Error())
		}
		c.debug("Setting port to %d", port)
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
			go c.handleClient(conn)
			break
		case <-c.proximityCache.ticker:
			c.debug("Emptying proximity cache...")
			go c.clearProximityCache()
			break
		}
	}
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
		if msg.Purpose > NODE_ANN {
			c.deliver(msg)
		}
		return nil
	}
	forward := c.forward(msg, target.ID)
	if forward {
		err = c.send(msg, target)
		if err == deadNodeError {
			err = c.remove(target.ID)
		}
		return err
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

// Join expresses a Node's desire to join the Cluster, kicking off a process that will populate its child leafSet, neighborhoodSet and routingTable. Once that process is complete, the Node can be said to be fully participating in the Cluster.
//
// The IP and port passed to Join should be those of a known Node in the Cluster. The algorithm assumes that the known Node is close in proximity to the current Node, but that is not a hard requirement.
func (c *Cluster) Join(addr multiaddr.Multiaddr) error {
	///func (c *Cluster) Join(ip string, port int) error {
	credentials := c.marshalCredentials()
	msg := c.NewMessage(NODE_JOIN, c.self.ID, credentials)
	return c.SendToIP(msg, addr)
}

func (c *Cluster) fanOutError(err error) {
	c.debug(err.Error())
	c.lock.RLock()
	defer c.lock.RUnlock()
	c.err(err.Error())
	for _, app := range c.applications {
		app.OnError(err)
	}
}

func (c *Cluster) sendHeartbeats() {
	msg := c.NewMessage(HEARTBEAT, c.self.ID, []byte{})
	nodes := c.table.list([]int{}, []int{})
	nodes = append(nodes, c.leafset.list()...)
	nodes = append(nodes, c.neighborhoodset.list()...)
	sent := map[NodeID]bool{}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if _, set := sent[node.ID]; set {
			continue
		}
		c.debug("Sending heartbeat to %s", node.ID)
		err := c.send(msg, node)
		if err == deadNodeError {
			err = c.remove(node.ID)
			if err != nil {
				c.fanOutError(err)
			}
			continue
		}
		sent[node.ID] = true
	}
}

func (c *Cluster) deliver(msg Message) {
	if msg.Purpose <= NODE_ANN {
		c.warn("Received utility message %s to the deliver function. Purpose was %d.", msg.Key, msg.Purpose)
		return
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
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
		fmt.Println("dings")
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
		node, _ := c.get(msg.Sender.ID)
		if node != nil {
			node.updateLastHeardFrom()
		}
	}
	conn.Write([]byte(`{"status": "Received."}`))
	c.debug("Got message with purpose %v", msg.Purpose)
	msg.Hop = msg.Hop + 1
	switch msg.Purpose {
	case NODE_JOIN:
		c.onNodeJoin(msg)
		break
	case NODE_ANN:
		c.onNodeAnnounce(msg)
		break
	case NODE_EXIT:
		c.onNodeExit(msg)
		break
	case HEARTBEAT:
		c.lock.RLock()
		defer c.lock.RUnlock()
		for _, app := range c.applications {
			app.OnHeartbeat(msg.Sender)
		}
		break
	case STAT_DATA:
		c.onStateReceived(msg)
		break
	case STAT_REQ:
		c.onStateRequested(msg)
		break
	case NODE_RACE:
		c.onRaceCondition(msg)
		break
	case NODE_REPR:
		c.onRepairRequest(msg)
		break
	default:
		c.onMessageReceived(msg)
	}
}

func (c *Cluster) send(msg Message, destination *Node) error {
	if destination == nil {
		return errors.New("Can't send to a nil node.")
	}
	if c.self == nil {
		return errors.New("Can't send from a nil node.")
	}
	address := c.GetIP(*destination)
	c.debug("Sending message %s with purpose %d to %s", msg.Key, msg.Purpose, address)
	start := time.Now()
	err := c.SendToIP(msg, address)
	if err == nil {
		proximity := time.Since(start)
		destination.setProximity(int64(proximity))
		destination.updateLastHeardFrom()
	}
	return err
}

// SendToIP sends a message directly to an IP using the Wendy networking logic.
func (c *Cluster) SendToIP(msg Message, maddr multiaddr.Multiaddr) error {
	c.debug("Sending message %s", string(msg.Value))
	conn, err := manet.Dial(maddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Duration(c.getNetworkTimeout()) * time.Second))
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(msg)
	if err != nil {
		return err
	}
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
	mask := StateMask{
		Mask: rT,
		Rows: []int{},
		Cols: []int{},
	}
	row := c.self.ID.CommonPrefixLen(msg.Key)
	if msg.Hop == 1 {
		// send only the matching routing table rows
		for i := 0; i < row; i++ {
			mask.Rows = append(mask.Rows, i)
			msg.Hop++
		}
		// also send neighborhood set, if I'm the first node to get the message
		mask.Mask = mask.Mask | nS
	} else {
		// send only the routing table rows that match the hop
		if msg.Hop < row {
			mask.Rows = append(mask.Rows, msg.Hop)
		}
	}
	next, err := c.Route(msg.Key)
	if err != nil {
		c.fanOutError(err)
	}
	eol := false
	if next == nil {
		// also send leaf set, if I'm the last node to get the message
		mask.Mask = mask.Mask | lS
		eol = true
	}
	err = c.sendStateTables(msg.Sender, mask, eol)
	if err != nil {
		if err != deadNodeError {
			c.fanOutError(err)
		}
	}
	// forward the message on to the next destination
	err = c.Send(msg)
	if err != nil {
		c.fanOutError(err)
	}
}

// A node has joined the cluster. We need to decide if it belongs in our state tables and if the nodes in the state tables it sends us belong in our state tables. If the version of our state tables it sends to us doesn't match our local version, we need to resend our state tables to prevent a race condition.
func (c *Cluster) onNodeAnnounce(msg Message) {
	c.debug("\0333[4;31mNode %s announced its presence!\033[0m", msg.Key)
	conflicts := byte(0)
	if c.self.leafsetVersion > msg.LSVersion {
		c.debug("Expected LSVersion %d, got %d", c.self.leafsetVersion, msg.LSVersion)
		conflicts = conflicts | lS
	}
	if c.self.routingTableVersion > msg.RTVersion {
		c.debug("Expected RTVersion %d, got %d", c.self.routingTableVersion, msg.RTVersion)
		conflicts = conflicts | rT
	}
	if c.self.neighborhoodSetVersion > msg.NSVersion {
		c.debug("Expected NSVersion %d, got %d", c.self.neighborhoodSetVersion, msg.NSVersion)
		conflicts = conflicts | nS
	}
	if conflicts > 0 {
		c.debug("Uh oh, %s hit a race condition. Resending state.", msg.Key)
		err := c.sendRaceNotification(msg.Sender, StateMask{Mask: conflicts})
		if err != nil {
			c.fanOutError(err)
		}
		return
	}
	c.debug("No conflicts!")
	err := c.insertMessage(msg)
	if err != nil {
		c.fanOutError(err)
	}
	c.debug("About to fan out join messages...")
	c.fanOutJoin(msg.Sender)
}

func (c *Cluster) onNodeExit(msg Message) {
	c.debug("Node %s left. :(", msg.Sender.ID)
	err := c.remove(msg.Sender.ID)
	if err != nil {
		c.fanOutError(err)
		return
	}
}

func (c *Cluster) onStateReceived(msg Message) {
	err := c.insertMessage(msg)
	if err != nil {
		c.debug(err.Error())
		c.fanOutError(err)
	}
	var state stateTables
	err = json.Unmarshal(msg.Value, &state)
	if err != nil {
		c.debug(err.Error())
		c.fanOutError(err)
		return
	}
	c.debug("State received. EOL is %v, isJoined is %v.", state.EOL, c.isJoined())
	if !c.isJoined() && state.EOL {
		c.debug("Haven't announced presence yet... waiting %d seconds", (2 * c.getNetworkTimeout()))
		time.Sleep(time.Duration(2*c.getNetworkTimeout()) * time.Second)
		err = c.announcePresence()
		if err != nil {
			c.fanOutError(err)
		}
	} else if !state.EOL {
		c.debug("Already announced presence.")
	} else {
		c.debug("Not end of line.")
	}
}

func (c *Cluster) onStateRequested(msg Message) {
	c.debug("%s wants to know about my state tables!", msg.Sender.ID)
	var mask StateMask
	err := json.Unmarshal(msg.Value, &mask)
	if err != nil {
		c.fanOutError(err)
		return
	}
	c.sendStateTables(msg.Sender, mask, false)
}

func (c *Cluster) onRaceCondition(msg Message) {
	c.debug("Race condition. Awkward.")
	err := c.insertMessage(msg)
	if err != nil {
		c.fanOutError(err)
	}
	err = c.announcePresence()
	if err != nil {
		c.fanOutError(err)
	}
}

func (c *Cluster) onRepairRequest(msg Message) {
	c.debug("Helping to repair %s", msg.Sender.ID)
	var mask StateMask
	err := json.Unmarshal(msg.Value, &mask)
	if err != nil {
		c.fanOutError(err)
		return
	}
	c.sendStateTables(msg.Sender, mask, false)
}

func (c *Cluster) onMessageReceived(msg Message) {
	c.debug("Received message %s", msg.Key)
	err := c.Send(msg)
	if err != nil {
		c.fanOutError(err)
	}
}

func (c *Cluster) dumpStateTables(tables StateMask) (stateTables, error) {
	var state stateTables
	if tables.includeRT() {
		state.RoutingTable = c.table.export(tables.Rows, tables.Cols)
	}
	if tables.includeLS() {
		state.LeafSet = c.leafset.export()
	}
	if tables.includeNS() {
		state.NeighborhoodSet = c.neighborhoodset.export()
	}
	return state, nil
}

func (c *Cluster) sendStateTables(node Node, tables StateMask, eol bool) error {
	state, err := c.dumpStateTables(tables)
	if err != nil {
		return err
	}
	state.EOL = eol
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	msg := c.NewMessage(STAT_DATA, c.self.ID, data)
	target, err := c.get(node.ID)
	if err != nil {
		if _, ok := err.(IdentityError); !ok && err != nodeNotFoundError {
			return err
		} else if err == nodeNotFoundError {
			return c.send(msg, &node)
		}
	}
	c.debug("Sending state tables to %s", node.ID)
	return c.send(msg, target)
}

func (c *Cluster) sendRaceNotification(node Node, tables StateMask) error {
	state, err := c.dumpStateTables(tables)
	if err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	msg := c.NewMessage(NODE_RACE, c.self.ID, data)
	target, err := c.get(node.ID)
	if err != nil {
		if _, ok := err.(IdentityError); !ok && err != nodeNotFoundError {
			return err
		} else if err == nodeNotFoundError {
			return c.send(msg, &node)
		}
	}
	c.debug("Sending state tables to %s to fix race condition", node.ID)
	return c.send(msg, target)
}

func (c *Cluster) announcePresence() error {
	c.debug("Announcing presence...")
	state, err := c.dumpStateTables(StateMask{Mask: all})
	if err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	msg := c.NewMessage(NODE_ANN, c.self.ID, data)
	nodes := c.table.list([]int{}, []int{})
	nodes = append(nodes, c.leafset.list()...)
	nodes = append(nodes, c.neighborhoodset.list()...)
	sent := map[NodeID]bool{}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		c.debug("Saw node %s. rtVersion: %d\tlsVersion: %d\tnsVersion: %d", node.ID.String(), node.routingTableVersion, node.leafsetVersion, node.neighborhoodSetVersion)
		if _, set := sent[node.ID]; set {
			c.debug("Skipping node %s, already sent announcement there.", node.ID.String())
			continue
		}
		c.debug("Announcing presence to %s", node.ID)
		c.debug("Node: %s\trt: %d\tls: %d\tns: %d", node.ID.String(), node.routingTableVersion, node.leafsetVersion, node.neighborhoodSetVersion)
		msg.LSVersion = node.leafsetVersion
		msg.RTVersion = node.routingTableVersion
		msg.NSVersion = node.neighborhoodSetVersion
		err := c.send(msg, node)
		if err == deadNodeError {
			err = c.remove(node.ID)
			if err != nil {
				c.fanOutError(err)
			}
			continue
		}
		sent[node.ID] = true
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.joined = true
	return nil
}

func (c *Cluster) repairLeafset(id NodeID) error {
	target, err := c.leafset.getNextNode(id)
	if err != nil {
		if err == nodeNotFoundError {
			c.warn("No node found when trying to repair the leafset. Was there a catastrophe?")
		} else {
			return err
		}
	}
	mask := StateMask{Mask: lS}
	data, err := json.Marshal(mask)
	if err != nil {
		return err
	}
	msg := c.NewMessage(NODE_REPR, id, data)
	return c.send(msg, target)
}

func (c *Cluster) repairTable(id NodeID) error {
	row := c.self.ID.CommonPrefixLen(id)
	reqRow := row
	col := int(id.Digit(row))
	targets := []*Node{}
	for len(targets) < 1 && row < len(c.table.nodes) {
		targets = c.table.list([]int{row}, []int{})
		if len(targets) < 1 {
			row = row + 1
		}
	}
	mask := StateMask{Mask: rT, Rows: []int{reqRow}, Cols: []int{col}}
	data, err := json.Marshal(mask)
	if err != nil {
		return err
	}
	msg := c.NewMessage(NODE_REPR, c.self.ID, data)
	for _, target := range targets {
		err = c.send(msg, target)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) repairNeighborhood() error {
	targets := c.neighborhoodset.list()
	mask := StateMask{Mask: nS}
	data, err := json.Marshal(mask)
	if err != nil {
		return err
	}
	msg := c.NewMessage(NODE_REPR, c.self.ID, data)
	for _, target := range targets {
		err = c.send(msg, target)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) updateProximity(node *Node) error {
	proximity := c.getCachedProximity(node.ID)
	if proximity < 0 {
		msg := c.NewMessage(HEARTBEAT, c.self.ID, []byte{})
		c.debug("Checking proximity to %s", node.ID)
		err := c.send(msg, node)
		if err != nil {
			return err
		}
		c.debug("Proximity to %s checked.", node.ID)
		c.cacheProximity(node.ID, node.getRawProximity())
		c.debug("Proximity to %s cached.", node.ID)
	}
	return nil
}

func (c *Cluster) insertMessage(msg Message) error {
	var state stateTables
	err := json.Unmarshal(msg.Value, &state)
	if err != nil {
		c.debug("Error unmarshalling JSON: %s", err.Error())
		return err
	}
	sender := &msg.Sender
	c.debug("Updating versions for %s. RT: %d, LS: %d, NS: %d.", sender.ID.String(), msg.RTVersion, msg.LSVersion, msg.NSVersion)
	sender.updateVersions(msg.RTVersion, msg.LSVersion, msg.NSVersion)
	err = c.insert(*sender, StateMask{Mask: all})
	if err != nil {
		return err
	}
	for _, state := range state.NeighborhoodSet {
		err = c.insert(state.Node, StateMask{Mask: nS})
		if err != nil {
			return err
		}
	}
	for _, state := range state.LeafSet {
		err = c.insert(state.Node, StateMask{Mask: lS | nS})
		if err != nil {
			return err
		}
	}
	for _, state := range state.RoutingTable {
		err = c.insert(state.Node, StateMask{Mask: rT | nS})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) insert(node Node, tables StateMask) error {
	if node.IsZero() {
		return nil
	}
	if node.ID.Equals(c.self.ID) {
		c.debug("Skipping inserting myself.")
		return nil
	}
	c.debug("Inserting node %s", node.ID)
	if node.getRawProximity() <= 0 && (tables.includeNS() || tables.includeRT()) {
		c.debug("Updating proximity")
		c.updateProximity(&node)
		c.debug("Updated proximity")
		c.debug("Inserting node %s in routing table.", node.ID)
		resp, err := c.table.insertNode(node, node.getRawProximity())
		if err != nil && err != rtDuplicateInsertError {
			c.err("Error inserting node: %s", err.Error())
			return err
		}
		if resp != nil && err != rtDuplicateInsertError {
			c.debug("Inserted node %s in routing table.", resp.ID)
		}
		if err == rtDuplicateInsertError {
			c.debug(err.Error())
		}
	}
	if tables.includeLS() {
		c.debug("Inserting node %s in leaf set.", node.ID)
		resp, err := c.leafset.insertNode(node)
		if err != nil && err != lsDuplicateInsertError {
			return err
		}
		if resp != nil && err != lsDuplicateInsertError {
			c.debug("Inserted node %s in leaf set.", resp.ID)
			c.newLeaves(c.leafset.list())
		}
		c.debug("At the end of the leafset insert block.")
		if err == lsDuplicateInsertError {
			c.debug(err.Error())
		}
	}
	if tables.includeNS() {
		c.debug("Inserting node %s in neighborhood set.", node.ID)
		resp, err := c.neighborhoodset.insertNode(node, node.getRawProximity())
		if err != nil && err != nsDuplicateInsertError {
			return err
		}
		if resp != nil && err != nsDuplicateInsertError {
			c.debug("Inserted node %s in neighborhood set.", resp.ID)
		}
		if err == nsDuplicateInsertError {
			c.debug(err.Error())
		}
	}
	return nil
}

func (c *Cluster) remove(id NodeID) error {
	resp, err := c.table.removeNode(id)
	if err != nil {
		return err
	}
	if resp != nil {
		err = c.repairTable(resp.ID)
		if err != nil {
			return err
		}
	}
	resp, err = c.leafset.removeNode(id)
	if err != nil {
		return err
	}
	if resp != nil {
		err = c.repairLeafset(resp.ID)
		if err != nil {
			return err
		}
		c.newLeaves(c.leafset.list())
	}
	resp, err = c.neighborhoodset.removeNode(id)
	if err != nil {
		return err
	}
	if resp != nil {
		err = c.repairNeighborhood()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) get(id NodeID) (*Node, error) {
	node, err := c.neighborhoodset.getNode(id)
	if err == nodeNotFoundError {
		node, err = c.leafset.getNode(id)
		if err == nodeNotFoundError {
			node, err = c.table.getNode(id)
			return node, err
		}
		return node, err
	}
	return node, err
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
