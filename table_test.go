package pastry

import (
	"testing"
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
