package pastry

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Application is an interface that other packages can fulfill to interact with Pastry.
type Application interface {
	OnError(err error)
	OnDeliver(msg Message)
	OnForward(msg *Message, nextId NodeID) bool // return False if Pastry should not forward
	OnNewLeafs(leafset []*Node)
	OnNodeJoin(node Node)
	OnNodeExit(node Node)
	OnHeartbeat(node Node)
}

// Cluster holds the information about the state of the network. It is the main point of interaction for the network.
type Cluster struct {
	self            *Node
	table           *RoutingTable
	leafset         *LeafSet
	req             chan *Message
	kill            chan bool
	lastStateUpdate time.Time
	applications    []Application
}

func (c *Cluster) ID() NodeID {
	return c.self.ID
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
		applications:    []Application{},
	}
}

// Stop shuts down the local listener for the Cluster, preventing it from receiving or sending further messages.
func (c *Cluster) Stop() {
	c.table.Stop()
	c.leafset.Stop()
	c.kill <- true
}

func (c *Cluster) RegisterCallback(app Application) {
	c.applications = append(c.applications, app)
}

// Listen starts the Cluster listening for events, including all the individual listeners for each state object.
func (c *Cluster) Listen() {
	go c.table.listen()
	go c.leafset.listen()
	go c.listen(c.self.Port)
	for {
		time.Sleep(1 * time.Second)
		c.sendHeartbeat()
	}

}

func (c *Cluster) listen(port int) {
	portstr := strconv.Itoa(port)
	ln, err := net.Listen("tcp", ":"+portstr)
	if err != nil {
		panic(err.Error())
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err.Error())
		}
		go c.handleClient(conn)
	}
}

func (c *Cluster) sendHeartbeat() {
	msg := Message{
		Purpose: NODE_TEST,
		Origin:  *c.self,
		Hops:    []NodeID{c.self.ID},
		Key:     c.self.ID,
		Value:   "",
		Sent:    time.Now(),
	}
	nodes, err := c.table.Dump()
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	leaves, err := c.leafset.Dump()
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	nodes = append(nodes, leaves...)
	for _, node := range nodes {
		go func(n *Node) {
			err = c.self.Send(msg, n)
			if err == deadNodeError {
				c.nodeExit(Message{Origin: *n})
			} else if err != nil{
				for _, app := range c.applications {
					app.OnError(err)
				}
			}
		}(node)
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
		conn.Write([]byte("alive"))
		break
	case NODE_STAT:
		c.nodeStateReceived(msg)
		break
	case NODE_REPR:
		c.sendLeafSet(msg)
		break
	default:
		c.messageReceived(msg, true)
		break
	}
}

// nodeJoin handles new node arrivals in the cluster.
func (c *Cluster) nodeJoin(msg Message) {
	leaf_nodes, err := c.leafset.Dump()
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	table_nodes, err := c.table.Dump()
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	nodes := append(leaf_nodes, table_nodes...)
	data, err := json.Marshal(&nodes)
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	response := Message{
		Purpose: NODE_STAT,
		Origin:  *c.self,
		Hops:    []NodeID{c.self.ID},
		Key:     msg.Key,
		Value:   string(data),
		Sent:    time.Now(),
	}
	err = c.self.Send(response, &msg.Origin)
	if err != nil {
		fmt.Println("Node died before we could send it state.")
		c.nodeExit(msg)
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	_, err = c.leafset.Insert(&msg.Origin)
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	_, err = c.table.Insert(&msg.Origin)
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	for _, app := range c.applications {
		app.OnNodeJoin(msg.Origin)
	}
}

// nodeExit handles node departures in the cluster.
func (c *Cluster) nodeExit(msg Message) {
	_, err := c.table.Remove(&msg.Origin, -1, -1, -1)
	if err != nil {
		for _, application := range c.applications {
			application.OnError(err)
		}
	}
	req, err := c.leafset.Remove(&msg.Origin, -1, false)
	if err != nil {
		for _, application := range c.applications {
			application.OnError(err)
		}
	}
	if req != nil {
		repairMsg := Message{
			Purpose: NODE_REPR,
			Origin:  *c.self,
			Hops:    []NodeID{c.self.ID},
			Key:     c.self.ID,
			Value:   "",
			Sent:    time.Now(),
		}
		repairSource, err := c.leafset.Scan(msg.Origin.ID)
		if err != nil {
			for _, app := range c.applications {
				app.OnError(err)
			}
		}
		if repairSource == nil || repairSource.Node == nil {
			panic("Half the leafset is empty!")
		}
		err = c.self.Send(repairMsg, repairSource.Node)
		if err == deadNodeError {
			repairMsg.Origin = *(repairSource.Node)
			c.nodeExit(repairMsg)
		} else {
			for _, app := range c.applications {
				app.OnError(err)
			}

		}
		dump, err := c.leafset.Dump()
		if err != nil {
			for _, app := range c.applications {
				app.OnError(err)
			}
		}
		for _, app := range c.applications {
			app.OnNewLeafs(dump)
		}
	}
	for _, app := range c.applications {
		app.OnNodeExit(msg.Origin)
	}
}

// nodeHeartbeat handles messages that just serve to see if the node is still alive.
func (c *Cluster) nodeHeartbeat(msg Message) {
	diff := time.Since(msg.Sent)
	req, err := c.table.Get(&msg.Origin, -1, -1, -1)
	if err != nil {
		for _, application := range c.applications {
			application.OnError(err)
		}
	}
	if req == nil || req.Node == nil {
		req, err = c.table.Insert(&msg.Origin)
		if err != nil {
			for _, application := range c.applications {
				application.OnError(err)
			}
		}
	}
	fmt.Printf("Setting proximity of %s to %d.\n", msg.Origin.ID.String(), diff.Nanoseconds())
	req.Node.setProximity(diff.Nanoseconds())
	for _, app := range c.applications {
		app.OnHeartbeat(msg.Origin)
	}
}

// BUG(paddyforan): If the Nodes don't agree on the time, Pastry can create an infinite loop. Workaround: Use NTP, for the love of God!

// nodeStateReceived handles messages that are broadcast when a node's state is updated, and can be used to update the local node's state tables with the relevant information, if there is any.
func (c *Cluster) nodeStateReceived(msg Message) {
	var data []Node
	err := json.Unmarshal([]byte(msg.Value), &data)
	if err != nil {
		for _, application := range c.applications {
			application.OnError(err)
		}
	}
	if c.lastStateUpdate.After(msg.Sent) {
		fmt.Println("Detected race condition; " + msg.Origin.ID.String() + " sent the message at " + msg.Sent.String() + " but we last updated state at " + c.lastStateUpdate.String() + ". Notifying " + msg.Origin.ID.String() + ".")
		conflictMsg := Message{
			Purpose: NODE_RACE,
			Origin:  *c.self,
			Hops:    []NodeID{c.self.ID},
			Key:     msg.Key,
			Value:   c.lastStateUpdate.String(),
			Sent:    time.Now(),
		}
		err = c.self.Send(conflictMsg, &msg.Origin)
		if err != nil {
			if err == deadNodeError {
				fmt.Println("Node died before we could tell it about the race condition.")
				c.nodeExit(msg)
			} else {
				for _, app := range c.applications {
					app.OnError(err)
				}
			}
		}
	}
	//updatedLeafset := false
	//updatedTable := false
	//data = append(data, msg.Origin)
	for _, node := range data {
		_, err := c.leafset.Insert(&node)
		if err != nil {
			for _, application := range c.applications {
				application.OnError(err)
			}
		}
		/*if req != nil {
			updatedLeafset = true
		}*/
		_, err = c.table.Insert(&node)
		if err != nil {
			for _, application := range c.applications {
				application.OnError(err)
			}
		}
		/*if req2 != nil {
			updatedTable = true
		}*/
	}/*
	var nodes, leafset, table []*Node
	/*if updatedLeafset {
		dump, err := c.leafset.Dump()
		if err != nil {
			for _, app := range c.applications {
				app.OnError(err)
			}
		}
		for _, app := range c.applications {
			app.OnNewLeafs(dump)
		}
		nodes = append(nodes, dump...)
		leafset = dump
	}
	if updatedTable {
		dump, err := c.table.Dump()
		if err != nil {
			for _, app := range c.applications {
				app.OnError(err)
			}
		}
		nodes = append(nodes, dump...)
		table = dump
	}
	if updatedTable { || updatedLeafset {
		if !updatedLeafset {
			leafset, err = c.leafset.Dump()
			if err != nil {
				for _, app := range c.applications {
					app.OnError(err)
				}
			}
		}
		if !updatedTable {
			table, err = c.table.Dump()
			if err != nil {
				for _, app := range c.applications {
					app.OnError(err)
				}
			}
		}
		updatelist := append(leafset, table...)
		data, err := json.Marshal(table)
		if err != nil {
			for _, app := range c.applications {
				app.OnError(err)
			}
		}
		updateMsg := Message{
			Purpose: NODE_STAT,
			Origin:  *c.self,
			Hops:    []NodeID{c.self.ID},
			Key:     c.self.ID,
			Value:   string(data),
			Sent:    time.Now(),
		}
		for _, node := range table {
			fmt.Printf("%s: Sending msg to %s\n", c.self.ID, node.ID)
			err = c.self.Send(updateMsg, node)
			if err != nil {
				for _, app := range c.applications {
					app.OnError(err)
				}
			}
		}
	}*/
}

// messageReceived handles messages that are intended for applications built on Pastry.
func (c *Cluster) messageReceived(msg Message, addSelf bool) {
	next, err := c.leafset.route(msg.Key)
	if err != nil {
		for _, application := range c.applications {
			application.OnError(err)
		}
	}
	if next == nil {
		next, err = c.table.route(msg.Key)
		if err != nil {
			for _, application := range c.applications {
				application.OnError(err)
			}
		}
	}
	if next == nil {
		for _, app := range c.applications {
			app.OnDeliver(msg)
		}
		return
	}
	forward := true
	for _, app := range c.applications {
		f := app.OnForward(&msg, next.ID)
		if forward {
			forward = f
		}
	}
	if forward {
		if addSelf {
			msg.Hops = append(msg.Hops, c.self.ID)
		}
		err = c.self.Send(msg, next)
		if err != nil {
			if err == deadNodeError {
				failedMsg := Message{
					Origin: *next,
				}
				c.nodeExit(failedMsg)
				c.messageReceived(msg, false)
				return
			}
			for _, application := range c.applications {
				application.OnError(err)
			}
		}
	} else {
		fmt.Println("Message halted because of OnForward.")
	}
}

func (c *Cluster) sendLeafSet(msg Message) {
	fmt.Println(c.self.ID.String() + ": Sending leaf set...")
	dump, err := c.leafset.Dump()
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	leaves, err := json.Marshal(dump)
	if err != nil {
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
	repairMessage := Message{
		Purpose: NODE_STAT,
		Origin:  *c.self,
		Hops:    []NodeID{c.self.ID},
		Key:     msg.Key,
		Value:   string(leaves),
		Sent:    time.Now(),
	}
	err = c.self.Send(repairMessage, &msg.Origin)
	if err != nil {
		if err == deadNodeError {
			fmt.Println("Node died before we could send it leafset.")
			c.nodeExit(msg)
			return
		}
		for _, app := range c.applications {
			app.OnError(err)
		}
	}
}

func (c *Cluster) Join(ip string, port int) error {
	msg := Message{
		Purpose: NODE_JOIN,
		Origin:  *c.self,
		Hops:    []NodeID{c.self.ID},
		Key:     c.self.ID,
		Value:   "",
		Sent:    time.Now(),
	}
	target := NewNode(c.self.ID, ip, ip, c.self.Region, port)
	err := c.self.Send(msg, target)
	if err != nil && err == deadNodeError {
		panic("Trying to join dead cluster.")
	}
	return err
}

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
		for _, app := range c.applications {
			app.OnDeliver(msg)
		}
		return nil
	}
	err = c.self.Send(msg, target)
	if err != nil {
		if err == deadNodeError {
			fmt.Println("Dead node. :(")
			c.nodeExit(Message{Origin: *target})
			return c.Send(msg)
		}
	}
	return err
}
