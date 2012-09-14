package pastry

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Cluster holds the information about the state of the network. It is the main point of interaction for the network.
type Cluster struct {
	self            *Node
	table           *RoutingTable
	leafset         *LeafSet
	req             chan *Message
	kill            chan bool
	lastStateUpdate time.Time
}

// NewCluster creates a new instance of a connection to the network and all the state tables for it.
func NewCluster(self *Node) *Cluster {
	table := NewRoutingTable(self)
	leafset := NewLeafSet(self)
	req := make(chan *Message)
	kill := make(chan bool)
	return &Cluster{
		self:            self,
		table:           table,
		leafset:         leafset,
		req:             req,
		kill:            kill,
		lastStateUpdate: time.Now(),
	}
}

// Stop shuts down the local listener for the Cluster, preventing it from receiving or sending further messages.
func (c *Cluster) Stop() {
	c.table.Stop()
	c.leafset.Stop()
	c.kill <- true
}

func (c *Cluster) listen(port int) {
	portstr := strconv.Itoa(port)
	ln, err := net.Listen("tcp", ":"+portstr)
	if err != nil {
		panic(err.Error())
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		go c.handleClient(conn)
	}
}

func (c *Cluster) handleClient(conn net.Conn) {
	defer conn.Close()
	var msg Message
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&msg)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	switch msg.Purpose {
	case NODE_JOIN:
		c.nodeJoin(msg)
		break
	case NODE_EXIT:
		c.nodeExit(msg)
		break
	case NODE_TEST:
		c.nodeHeartbeat(msg)
		break
	case NODE_STAT:
		c.nodeStateReceived(msg)
		break
	default:
		c.messageReceived(msg)
		break
	}
}

// Listen starts the Cluster listening for events, including all the individual listeners for each state object.
func (c *Cluster) Listen() {
	go c.table.listen()
	go c.leafset.listen()
	c.listen(c.self.Port)
}

// nodeJoin handles new node arrivals in the cluster.
func (c *Cluster) nodeJoin(msg Message) {
	fmt.Println("Received node join: " + msg.String())
	// TODO: tell msg.Origin all about your state tables
}

// nodeExit handles node departures in the cluster.
func (c *Cluster) nodeExit(msg Message) {
	fmt.Println("Received node exit: " + msg.String())
	_, err := c.table.Remove(msg.Origin, -1, -1, -1)
	if err != nil {
		// TODO: Deal with the timeout error
	}
	_, err := c.leafset.Remove(msg.Origin, -1, false)
	if err != nil {
		// TODO: Deal with the timeout error
	}
	// TODO: replenish leafset, if necessary
}

// nodeHeartbeat handles messages that just serve to see if the node is still alive.
func (c *Cluster) nodeHeartbeat(msg Message) {
	fmt.Println("Received node heartbeat: " + msg.String())
	diff := Since(msg.Sent)
	req, err := self.table.Get(msg.Origin, -1, -1, -1)
	if err != nil {
		// TODO: Deal with the timeout
	}
	if req == nil {
		// TODO: Deal with node not being in routing table
	}
	fmt.Printf("Setting proximity of %s to %d.\n", msg.Origin.ID.String().diff.Nanoseconds())
	req.Node.setProximity(diff.Nanoseconds())
	// TODO: Reply "I'm alive!"
}

// BUG(paddyforan): If the Nodes don't agree on the time, this method can create an infinite loop. Workaround: Use NTP, for the love of God!

// nodeStateReceived handles messages that are broadcast when a node's state is updated, and can be used to update the local node's state tables with the relevant information, if there is any.
func (c *Cluster) nodeStateReceived(msg Message) {
	fmt.Println("Received node state: " + msg.String())
	var data []Node
	err := json.Unmarshal([]byte(msg.Value), &data)
	if err != nil {
		// TODO: do something with this error
	}
	if c.lastStateUpdate.After(msg.Sent) {
		fmt.Println("Detected race condition; " + msg.Origin.ID.String() + " sent the message at " + msg.Sent.String() + " but we last updated state at " + c.lastStateUpdate.String() + ". Notifying " + msg.Origin.ID.String() + ".")
		// TODO: tell msg.Origin about the conflict so it can start over
	}
	for _, node := range data {
		_, err = c.leafset.Insert(node)
		if err != nil {
			// TODO: deal with this timeout
		}
		_, err = c.table.Insert(node)
		if err != nil {
			// TODO: deal with this timeout
		}
	}
}

// messageReceived handles messages that are intended for applications built on Pastry.
func (c *Cluster) messageReceived(msg Message) {
	// TODO: Really need to decide on an API for applications to receive callbacks from Pastry. I'm tempted to use a slice of channels that is periodically tested for closed channels, which will be removed, but that seems like unnecessary overhead

	fmt.Println("Received message: " + msg.String())
	next, err := c.leafset.route(msg.ID)
	if err != nil {
		// TODO: handle the timeout error
	}
	if next == nil {
		next, err = c.table.route(msg.ID)
		if err != nil {
			// TODO: handle the timeout error
		}
	}
	if next == nil {
		fmt.Println("Delivering message: " + msg.String())
		// TODO: call out to delivery handler
	}
	fmt.Println("Forwarding message '" + msg.String() + "' to Node " + next.ID.String())
	// TODO: Call out to forwardMessage callback
	err = c.self.Send(msg, next)
	if err != nil {
		// TODO: handle JSON encoding errors
		// TODO: handle nil self and destination error
	}
}
