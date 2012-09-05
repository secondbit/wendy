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
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	var msg Message
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&msg)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(msg) // TODO do something useful instead
}

// Listen starts the Cluster listening for events, including all the individual listeners for each state object.
func (c *Cluster) Listen() {
	go c.table.listen()
	go c.leafset.listen()
	go c.listen(c.self.Port)
}
