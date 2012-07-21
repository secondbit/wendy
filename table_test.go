package pastry

import (
	"strconv"
	"testing"
	"time"
)

// Test insertion and retrieval of nodes in the routing table
func TestRoutingTableGetSet(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	t.Logf("%s\n", self_id.String())

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	row := self_id.CommonPrefixLen(other_id)
	col := other_id[row].Canonical()
	t.Logf("%s\n", other_id.String())
	t.Logf("%v\n", row)
	t.Logf("%v\n", int(col))
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	table.Insert(other)
	node, err := table.GetNode(row, int(col), 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if node == nil {
		t.Fatalf("Nil Node returned.")
	}
	if !node.ID.Equals(other_id) {
		t.Errorf("Expected node ID of %s, got %s instead.", node.ID, other_id)
	}
}

// Test handling of a Node being inserted twice.
func TestRoutingTableDoubleSet(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	row := self_id.CommonPrefixLen(other_id)
	col := other_id[row].Canonical()
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	table.Insert(other)
	table.Insert(other)
	node, err := table.GetNode(row, int(col), 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if node == nil {
		t.Fatalf("Nil Node returned.")
	}
	if !node.ID.Equals(other_id) {
		t.Errorf("Expected node ID of %s, got %s instead.", node.ID, other_id)
	}
	second_node, err := table.GetNode(row, int(col), 1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if second_node != nil {
		t.Errorf("Expected nil node, got %s instead.", second_node.ID)
	}
}

// Test deleting the only node from column of the routing table
func TestRoutingTableDeleteOnly(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	row := self_id.CommonPrefixLen(other_id)
	col := other_id[row].Canonical()
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	table.Insert(other)
	node, err := table.GetNode(row, int(col), 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if node == nil {
		t.Fatalf("Nil Node returned.")
	}
	if !node.ID.Equals(other_id) {
		t.Fatalf("Expected node ID of %s, got %s instead.", node.ID, other_id)
	}

	err = table.RemoveNode(row, int(col), 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	node, err = table.GetNode(row, int(col), 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if node != nil {
		t.Errorf("Expected nil response, got Node %s instead.", node.ID)
	}
}

// TODO: Need to test deleting from the front of the entries list
// TODO: Need to test deleting from the end of the entries list
// TODO: Need to test deleting from the middle of the entries list

// Benchmarks

// How fast can we insert nodes
func BenchmarkRoutingTableInsert(b *testing.B) {
	b.StopTimer()
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		seed := strconv.FormatInt(time.Now().UnixNano()*int64(b.N), 10)
		b.StartTimer()
		other_id, err := NodeIDFromBytes([]byte(seed + seed + seed))
		if err != nil {
			b.Fatalf(err.Error())
		}
		other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
		table.Insert(other)
	}
}
