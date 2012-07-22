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

// TODO: Test getting a Node by its ID
// TODO: Test getting a Node by its position

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
		t.Errorf("Expected nil response, got Node %s instead.", r.Node.ID)
	}
}

// TODO: Need to test deleting the only Node using the ID

// TODO: Need to test deleting from the front of the entries list using the position
// TODO: Need to test deleting from the front of the entries list using the ID
// TODO: Need to test deleting from the end of the entries list using the position
// TODO: Need to test deleting from the end of the entries list using the ID
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
