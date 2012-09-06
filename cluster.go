package pastry

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
)

// Cluster holds the information about the state of the network. It is the main point of interaction for the network.
type Cluster struct {
	self    *Node
	table   *RoutingTable
	leafset *LeafSet
	req     chan *Message
	kill    chan bool
}

// NewCluster creates a new instance of a connection to the network and all the state tables for it.
func NewCluster(self *Node) *Cluster {
	table := NewRoutingTable(self)
	leafset := NewLeafSet(self)
	req := make(chan *Message)
	kill := make(chan bool)
	return &Cluster{
		self:    self,
		table:   table,
		leafset: leafset,
		req:     req,
		kill:    kill,
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
	// TODO: insert the node into the state tables (maybe?)
	fmt.Println("Received node join: " + msg.String())
}

// nodeExit handles node departures in the cluster.
func (c *Cluster) nodeExit(msg Message) {
	// TODO: remove the node from state tables
	// TODO: replenish leafset, if necessary
	fmt.Println("Received node exit: " + msg.String())
}

// nodeHeartbeat handles messages that just serve to see if the node is still alive.
func (c *Cluster) nodeHeartbeat(msg Message) {
	// TODO: reply with "I'm alive!"
	// TODO: update the sender's proximity, using msg.Sent
	fmt.Println("Received node heartbeat: " + msg.String())
}

// nodeStateReceived handles messages that are broadcast when a node's state is updated, and can be used to update the local node's state tables with the relevant information, if there is any.
func (c *Cluster) nodeStateReceived(msg Message) {
	// TODO: determine if any of the state information is relevant
	// TODO: update the relevant state information
	fmt.Println("Received node state: " + msg.String())
}

// messageReceived handles messages that are intended for applications built on Pastry.
func (c *Cluster) messageReceived(msg Message) {
	// TODO: Really need to decide on an API for applications to receive callbacks from Pastry. I'm tempted to use a slice of channels that is periodically tested for closed channels, which will be removed, but that seems like unnecessary overhead

	// TODO: route the message through the state tables
	// If the message should be forwarded:
	// TODO: Call out to forwardMessage callback
	// TODO: forward message
	// If this is the final destination:
	// TODO: Call out to delivery handler
	fmt.Println("Received message: " + msg.String())
}
