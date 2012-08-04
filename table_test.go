package pastry

import (
	"strconv"
	"testing"
	"time"
)

// Test insertion of a node into the routing table
func TestRoutingTableInsert(t *testing.T) {
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
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	if r.Row != row {
		t.Errorf("Expected node to be in row %d, was put in %d instead.", row, r.Row)
	}
	if r.Col != int(col) {
		t.Errorf("Expected node to be in column %d, was put in %d instead.", int(col), r.Col)
	}
}

// Test handling of a Node being inserted twice.
func TestRoutingTableDoubleInsert(t *testing.T) {
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
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("First insert returned a nil response.")
	}
	r2, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r2 == nil {
		t.Fatalf("Second insert returned a nil response.")
	}
	if r.Row != r2.Row {
		t.Errorf("Rows expected to be equal. %d != %d", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Errorf("Columns expected to be equal. %d != %d", r.Col, r2.Col)
	}
	if r.Entry != r2.Entry {
		t.Errorf("Entries expected to be equal. %d != %d", r.Entry, r2.Entry)
	}
}

// Test retrieving a Node by ID
func TestRoutingTableGetByID(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatal(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.Insert(other)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil response.")
	}
	r2, err := table.Get(other, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Returned nil response.")
	}
	if r2.Row != r.Row {
		t.Errorf("Expected row %v, got row %v.", r.Row, r2.Row)
	}
	if r2.Col != r.Col {
		t.Errorf("Expected column %v, got column %v.", r.Col, r2.Col)
	}
	if r2.Entry != r.Entry {
		t.Errorf("Expected entry %v, got entry %v.", r.Entry, r2.Entry)
	}
}

// Test retrieving a node by position
func TestRoutingTableGetByPos(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	other_id, err := NodeIDFromBytes([]byte("This is another test Node for testing purposes only."))
	if err != nil {
		t.Fatal(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.Insert(other)
	if err != nil {
		t.Fatal(err.Error())
	}
	r2, err := table.Get(nil, r.Row, r.Col, r.Entry)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Returned nil response.")
	}
	if !r2.Node.ID.Equals(other_id) {
		t.Errorf("Expected node ID of %s, got %s instead.", other_id, r2.Node.ID)
	}
}

// Test retrieving nodes by proximity
func TestRoutingTableGetClosestByProximity(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	other_id, err := NodeIDFromBytes([]byte("1234467890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing2", 55555)
	other.proximity = 10
	second_id, err := NodeIDFromBytes([]byte("1234467890abbdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	second := NewNode(second_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	second.proximity = 1
	r, err := table.Insert(other)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("First insert returned a nil response.")
	}
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Second insert returned a nil response.")
	}
	if r.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r.Col, r2.Col)
	}
	if r2.Entry != 1 {
		t.Fatalf("Second insert was inserted in correct position. Expected table[%v][%v][1], got table[%v][%v][%v].", r.Row, r.Col, r2.Row, r2.Col, r2.Entry)
	}
	t.Logf("First insert proximity: %v", table.self.Proximity(other))
	t.Logf("Second insert proximity: %v", table.self.Proximity(second))
	r3, err := table.GetByProximity(r.Row, r.Col, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Returned nil response.")
	}
	if !r3.Node.ID.Equals(second_id) {
		t.Errorf("Expected node ID of %s, got %s instead.", second_id, r3.Node.ID)
	}
	r4, err := table.GetByProximity(r.Row, r.Col, 1)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatal("Returned nil response.")
	}
	if !r4.Node.ID.Equals(other_id) {
		t.Errorf("Expected node ID of %s, got %s instead.", other_id, r4.Node.ID)
	}
}

// Test deleting the only node from column of the routing table using its position
func TestRoutingTableDeleteOnlyByPos(t *testing.T) {
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
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	_, err = table.Remove(nil, r.Row, r.Col, r.Entry)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := table.Get(nil, r.Row, r.Col, r.Entry)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
}

// Test deleting the only node from column of the routing table using its ID
func TestRoutingTableDeleteOnlyByID(t *testing.T) {
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
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	_, err = table.Remove(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := table.Get(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
}

// Test deleting the first of two nodes from a column of the routing table using its position
func TestRoutingTableDeleteFirstByPos(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	other_id, err := NodeIDFromBytes([]byte("1234557890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	second_id, err := NodeIDFromBytes([]byte("1234557890abbdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "127.0.0.3", "127.0.0.3", "testing", 55555)
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r.Col, r2.Col)
	}
	if r2.Entry != 1 {
		t.Fatalf("Second insert didn't get added to the end of the column. Expected 1, got %v.", r2.Entry)
	}
	_, err = table.Remove(nil, r.Row, r.Col, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := table.Get(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r4, err := table.Get(r2.Node, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for second insert.")
	}
	if r4.Entry != 0 {
		t.Errorf("Expected second insert to be in position 0, got %v instead.", r4.Entry)
	}
}

// Test deleting the first of two nodes from a column of the routing table using its ID
func TestRoutingTableDeleteFirstByID(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	other_id, err := NodeIDFromBytes([]byte("1234557890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	second_id, err := NodeIDFromBytes([]byte("1234557890abbdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "127.0.0.3", "127.0.0.3", "testing", 55555)
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r.Col, r2.Col)
	}
	if r2.Entry != 1 {
		t.Fatalf("Second insert didn't get added to the end of the column. Expected 1, got %v.", r2.Entry)
	}
	_, err = table.Remove(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := table.Get(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r4, err := table.Get(r2.Node, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for second insert.")
	}
	if r4.Entry != 0 {
		t.Errorf("Expected second insert to be in position 0, got %v instead.", r4.Entry)
	}
}

// Test deleting the last of multiple nodes from a column of the routing table using its position
func TestRoutingTableDeleteLastByPos(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	other_id, err := NodeIDFromBytes([]byte("1234557890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	second_id, err := NodeIDFromBytes([]byte("1234557890abbdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "127.0.0.3", "127.0.0.3", "testing", 55555)
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r.Col, r2.Col)
	}
	if r2.Entry != 1 {
		t.Fatalf("Second insert didn't get added to the end of the column. Expected 1, got %v.", r2.Entry)
	}
	_, err = table.Remove(nil, r2.Row, r2.Col, r2.Entry)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := table.Get(r2.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r4, err := table.Get(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	if r4.Entry != 0 {
		t.Errorf("Expected first insert to be in position 0, got %v instead.", r4.Entry)
	}
}

// Test deleting the last of multiple nodes from a column in the routing table based on its ID
func TestRoutingTableDeleteLastByID(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	other_id, err := NodeIDFromBytes([]byte("1234557890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	second_id, err := NodeIDFromBytes([]byte("1234557890abbdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "127.0.0.3", "127.0.0.3", "testing", 55555)
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r.Col, r2.Col)
	}
	if r2.Entry != 1 {
		t.Fatalf("Second insert didn't get added to the end of the column. Expected 1, got %v.", r2.Entry)
	}
	_, err = table.Remove(r2.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := table.Get(r2.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r4, err := table.Get(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	if r4.Entry != 0 {
		t.Errorf("Expected first insert to be in position 0, got %v instead.", r4.Entry)
	}
}

// Test deleting the middle of multiple nodes from a column of the routing table using its position
func TestRoutingTableDeleteMiddleByPos(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	other_id, err := NodeIDFromBytes([]byte("1234557890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	second_id, err := NodeIDFromBytes([]byte("1234557890abbdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "127.0.0.3", "127.0.0.3", "testing", 55555)
	third_id, err := NodeIDFromBytes([]byte("1234557890accdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	third := NewNode(third_id, "127.0.0.4", "127.0.0.4", "testing", 55555)
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r.Col, r2.Col)
	}
	if r2.Entry != 1 {
		t.Fatalf("Second insert didn't get added to the end of the column. Expected 1, got %v.", r2.Entry)
	}
	r3, err := table.Insert(third)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Nil response returned.")
	}
	if r3.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r2.Row, r3.Row)
	}
	if r3.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r2.Col, r3.Col)
	}
	if r3.Entry != 2 {
		t.Fatalf("Third insert didn't get added to the end of the column. Expected 2, got %v.", r3.Entry)
	}
	_, err = table.Remove(nil, r2.Row, r2.Col, 1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r4, err := table.Get(r2.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r4 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r5, err := table.Get(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r5 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	if r5.Entry != 0 {
		t.Errorf("Expected first insert to be in position 0, got %v instead.", r5.Entry)
	}
	r6, err := table.Get(r3.Node, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r6 == nil {
		t.Fatalf("Got nil response when querying for third insert.")
	}
	if r6.Entry != 1 {
		t.Errorf("Expected third insert to be in position 1, got %v instead.", r6.Entry)
	}
}

// Test deleting the middle of multiple nodes from a column of the routing table using its ID
func TestRoutingTableDeleteMiddleByID(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	other_id, err := NodeIDFromBytes([]byte("1234557890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	second_id, err := NodeIDFromBytes([]byte("1234557890abbdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "127.0.0.3", "127.0.0.3", "testing", 55555)
	third_id, err := NodeIDFromBytes([]byte("1234557890accdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	third := NewNode(third_id, "127.0.0.4", "127.0.0.4", "testing", 55555)
	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()
	r, err := table.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r.Col, r2.Col)
	}
	if r2.Entry != 1 {
		t.Fatalf("Second insert didn't get added to the end of the column. Expected 1, got %v.", r2.Entry)
	}
	r3, err := table.Insert(third)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Nil response returned.")
	}
	if r3.Row != r2.Row {
		t.Fatalf("Nodes not inserted in the same row. Expected %v, got %v.", r2.Row, r3.Row)
	}
	if r3.Col != r2.Col {
		t.Fatalf("Nodes not inserted in the same column. Expected %v, got %v.", r2.Col, r3.Col)
	}
	if r3.Entry != 2 {
		t.Fatalf("Third insert didn't get added to the end of the column. Expected 2, got %v.", r3.Entry)
	}
	_, err = table.Remove(r2.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r4, err := table.Get(r2.Node, 0, 0, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r4 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r5, err := table.Get(r.Node, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r5 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	if r5.Entry != 0 {
		t.Errorf("Expected first insert to be in position 0, got %v instead.", r5.Entry)
	}
	r6, err := table.Get(r3.Node, 0, 0, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r6 == nil {
		t.Fatalf("Got nil response when querying for third insert.")
	}
	if r6.Entry != 1 {
		t.Errorf("Expected third insert to be in position 1, got %v instead.", r6.Entry)
	}
}

// Test scanning the routing table when the key falls in between two nodes
func TestRoutingTableScanSplit(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	first_id, err := NodeIDFromBytes([]byte("12345677890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.Insert(first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("First insert returned nil.")
	}
	second_id, err := NodeIDFromBytes([]byte("12345637890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	second := NewNode(second_id, "127.0.0.3", "127.0.0.3", "testing", 55555)
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Second insert returned nil")
	}
	message_id, err := NodeIDFromBytes([]byte("12345657890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	d1 := message_id.Diff(first_id)
	d2 := message_id.Diff(second_id)
	if d1.Cmp(d2) != 0 {
		t.Fatalf("IDs not equidistant. Expected %s, got %s.", d1, d2)
	}
	r3, err := table.Scan(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Scan returned nil.")
	}
	if !second_id.Equals(r3.Node.ID) {
		t.Errorf("Wrong Node returned. Expected %s, got %s.", second_id, r3.Node.ID)
	}
}

// Test scanning the routing table when there are no suitable matches
func TestRoutingTableScanNone(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	first_id, err := NodeIDFromBytes([]byte("12345657890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	row := self_id.CommonPrefixLen(first_id)
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.Insert(first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234560890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	m_row := message_id.CommonPrefixLen(self_id)
	if row >= m_row {
		t.Fatalf("Node would be picked up by scan.")
	}
	r3, err := table.Scan(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 != nil {
		t.Errorf("Scan was supposed to return nil, returned %s instead.", r3.Node.ID)
	}
}

// Test scanning the routing table when there are multiple Nodes in the column
func TestRoutingTableScanMultipleEntries(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdge"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	first_id, err := NodeIDFromBytes([]byte("12345657890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing2", 55555)
	first.proximity = 10
	r, err := table.Insert(first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}

	second_id, err := NodeIDFromBytes([]byte("12345657890abcdf"))
	if err != nil {
		t.Fatal(err.Error())
	}
	second := NewNode(second_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	second.proximity = 1
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Second insert returned nil.")
	}
	if r.Row != r2.Row {
		t.Fatalf("Second was supposed to be in row %v, was put in row %v instead.", r.Row, r2.Row)
	}
	if r.Col != r2.Col {
		t.Fatalf("Second was supposed to be in column %v, was put in column %v instead.", r.Col, r2.Col)
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	m_row := message_id.CommonPrefixLen(self_id)
	if r.Row < m_row {
		t.Fatalf("Node wouldn't be picked up by scan.")
	}
	r3, err := table.Scan(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatalf("Scan returned nil.")
	}
	if !r3.Node.ID.Equals(second_id) {
		t.Errorf("Scan was supposed to return %s, returned %s instead.", second_id, r3.Node.ID)
	}
}

// Test scanning over multiple rows in the routing table
func TestRoutingTableScanMultipleRows(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890abdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.Insert(first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}

	second_id, err := NodeIDFromBytes([]byte("1234567890abcdff"))
	if err != nil {
		t.Fatal(err.Error())
	}
	second := NewNode(second_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r2, err := table.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Second insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890aaaaaaa"))
	if err != nil {
		t.Fatal(err.Error())
	}
	m_row := message_id.CommonPrefixLen(self_id)
	if r.Row < m_row || r2.Row < m_row {
		t.Fatalf("Node wouldn't be picked up by scan.")
	}
	if r.Row == m_row || r2.Row == m_row {
		t.Fatalf("Node inserted into the same row.\nNode one: %d\nNode two: %d\nMessage: %d\n", r.Row, r2.Row, m_row)
	}
	r3, err := table.Scan(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatalf("Scan returned nil.")
	}
	if !r3.Node.ID.Equals(first_id) {
		t.Errorf("Scan was supposed to return %s, returned %s instead.", first_id, r3.Node.ID)
	}
}

// Test routing to the only node in the routing table
func TestRoutingTableRouteOnly(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	row := self_id.CommonPrefixLen(first_id)
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.Insert(first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890adefgh"))
	if err != nil {
		t.Fatal(err.Error())
	}
	m_row := message_id.CommonPrefixLen(self_id)
	if row < m_row {
		t.Fatalf("Node wouldn't be picked up by routing.")
	}
	r3, err := table.route(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Route returned nil.")
	}
	if r3 == nil {
		t.Fatal("Route returned nil Node.")
	}
	if !r3.ID.Equals(first_id) {
		t.Fatalf("Expected Node %s, got Node %s instead.", first_id, r3.ID)
	}
}

// Test routing to a direct match in the routing table
func TestRoutingTableRouteMatch(t *testing.T) {
}

// Test routing when there are no suitable routing table matches
func TestRoutingTableRouteNone(t *testing.T) {
}

//////////////////////////////////////////////////////////////////////////
////////////////////////// Benchmarks ////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

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
		_, err = table.Insert(other)
		if err != nil {
			b.Fatalf(err.Error())
		}
	}
}

// How fast can we retrieve nodes by ID
func BenchmarkRoutingTableGetByID(b *testing.B) {
	b.StopTimer()
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	seed := strconv.FormatInt(time.Now().UnixNano(), 10)
	other_id, err := NodeIDFromBytes([]byte(seed + seed + seed))
	if err != nil {
		b.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	_, err = table.Insert(other)
	if err != nil {
		b.Fatalf(err.Error())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		table.Get(other, 0, 0, 0)
	}
}

// How fast can we retrieve nodes by position
func BenchmarkRoutingTableGetByPos(b *testing.B) {
	b.StopTimer()
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	seed := strconv.FormatInt(time.Now().UnixNano(), 10)
	other_id, err := NodeIDFromBytes([]byte(seed + seed + seed))
	if err != nil {
		b.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.Insert(other)
	if err != nil {
		b.Fatalf(err.Error())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		table.Get(nil, r.Row, r.Col, r.Entry)
	}
}

// How fast can we retrieve nodes by proximity
func BenchmarkRoutingTableGetByProximity(b *testing.B) {
	b.StopTimer()
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := NewRoutingTable(self)
	go table.listen()
	defer table.Stop()

	reqs := []*routingTableRequest{}

	for i := 0; i < b.N; i++ {
		seed := strconv.FormatInt(time.Now().UnixNano()*int64(i+1), 10)
		other_id, err := NodeIDFromBytes([]byte(seed + seed + seed))
		if err != nil {
			b.Fatalf(err.Error())
		}
		other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
		r, err := table.Insert(other)
		if err != nil {
			b.Fatalf(err.Error())
		}
		reqs = append(reqs, r)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		req := 0
		if len(reqs) > 1 {
			req = i % (len(reqs) - 1)
		}
		r := reqs[req]
		table.GetByProximity(r.Row, r.Col, 0)
	}
}
