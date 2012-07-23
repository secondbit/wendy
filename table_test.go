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

// TODO: Need to test deleting from the middle of the entries list using the position
// TODO: Need to test deleting from the middle of the entries list using the ID

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
