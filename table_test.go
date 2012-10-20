package pastry

import (
	"crypto/rand"
	"io"
	"strconv"
	"sync"
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
	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()
	r, err := table.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.getNode(other_id)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r2 == nil {
		t.Fatalf("Nil response returned.")
	}
	if !r2.ID.Equals(r.ID) {
		t.Fatalf("Expected %s, got %s.", r.ID, r2.ID)
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
	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()
	r, err := table.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("First insert returned a nil response.")
	}
	r2, err := table.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r2 != nil {
		t.Fatalf("Second insert did not return a nil response.")
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
	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()
	r, err := table.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	_, err = table.removeNode(other_id)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = table.getNode(r.ID)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatalf(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil instead.")
		}
	}
}

// Test deleting the first of two nodes from a column of the routing table
func TestRoutingTableDeleteFirst(t *testing.T) {
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
	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()
	r, err := table.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	r3, err := table.removeNode(other_id)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 == nil {
		t.Fatalf("Remove didn't remove a node.")
	}
	r4, err := table.getNode(other_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatalf(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r4 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r4.ID)
	}
	r5, err := table.getNode(second_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r5 == nil {
		t.Fatalf("Got nil response when querying for second insert.")
	}
}

// Test deleting the last of multiple nodes from a column in the routing table
func TestRoutingTableDeleteLast(t *testing.T) {
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
	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()
	r, err := table.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	r3, err := table.removeNode(second_id)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 == nil {
		t.Fatalf("Expected nil response, got Node %s", r3.ID)
	}
	r4, err := table.getNode(second_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatalf(err.Error())
		} else {
			t.Fatalf("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r4 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r4.ID)
	}
	r5, err := table.getNode(other_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r5 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
}

// Test deleting the middle of multiple nodes from a column of the routing table
func TestRoutingTableDeleteMiddle(t *testing.T) {
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
	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()
	r, err := table.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := table.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	r3, err := table.insertNode(*third)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Nil response returned.")
	}
	r4, err := table.removeNode(second_id)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r4 == nil {
		t.Fatal("No node removed.")
	}
	r5, err := table.getNode(second_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatalf(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r5 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r5.ID)
	}
	r6, err := table.getNode(other_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r6 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	r7, err := table.getNode(third_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r7 == nil {
		t.Fatalf("Got nil response when querying for third insert.")
	}
}

// Test routing when the key falls in between two nodes
func TestRoutingTableScanSplit(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	first_id, err := NodeIDFromBytes([]byte("12345677890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.insertNode(*first)
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
	r2, err := table.insertNode(*second)
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
	r3, err := table.route(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Scan returned nil.")
	}
	if !second_id.Equals(r3.ID) {
		t.Errorf("Wrong Node returned. Expected %s, got %s.", second_id, r3.ID)
	}
}

// Test routing when there are no suitable matches
func TestRoutingTableRouteNone(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	first_id, err := NodeIDFromBytes([]byte("12345657890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	row := self_id.CommonPrefixLen(first_id)
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.insertNode(*first)
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
	r3, err := table.route(message_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatal(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, didn't get an error.")
		}
	}
	if r3 != nil {
		t.Errorf("Scan was supposed to return nil, returned %s instead.", r3.ID)
	}
}

// Test routing when there are multiple Nodes in the column
func TestRoutingTableScanMultipleEntries(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdge"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	first_id, err := NodeIDFromBytes([]byte("12345657890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing2", 55555)
	r, err := table.insertNode(*first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	r.proximity = 10

	second_id, err := NodeIDFromBytes([]byte("12345657890abcdf"))
	if err != nil {
		t.Fatal(err.Error())
	}
	second := NewNode(second_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r2, err := table.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Second insert returned nil.")
	}
	r2.proximity = 1
	message_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	diff := self.ID.Diff(message_id)
	first_diff := self.ID.Diff(first_id)
	second_diff := self.ID.Diff(second_id)
	t.Log(diff)
	t.Log(first_diff.Cmp(diff))
	t.Log(second_diff.Cmp(diff))
	row := first_id.CommonPrefixLen(self_id)
	m_row := message_id.CommonPrefixLen(self_id)
	if row < m_row {
		t.Fatalf("Node wouldn't be picked up by scan.")
	}
	r3, err := table.route(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatalf("Scan returned nil.")
	}
	if !r3.ID.Equals(second_id) {
		t.Errorf("Scan was supposed to return %s, returned %s instead.", second_id, r3.ID)
	}
}

// Test routing over multiple rows in the routing table
func TestRoutingTableScanMultipleRows(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890abdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.insertNode(*first)
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
	r2, err := table.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Second insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890accdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first_row := first_id.CommonPrefixLen(self_id)
	second_row := second_id.CommonPrefixLen(self_id)
	m_row := message_id.CommonPrefixLen(self_id)
	if first_row < m_row || second_row < m_row {
		t.Fatalf("Node wouldn't be picked up by scan.")
	}
	if first_row == m_row || second_row == m_row {
		t.Fatalf("Node inserted into the same row.\nNode one: %d\nNode two: %d\nMessage: %d\n", first_row, second_row, m_row)
	}
	r3, err := table.route(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatalf("Scan returned nil.")
	}
	if !r3.ID.Equals(first_id) {
		t.Errorf("Scan was supposed to return %s, returned %s instead.", first_id, r3.ID)
	}
}

// Test routing to the only node in the routing table
func TestRoutingTableRouteOnly(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	row := self_id.CommonPrefixLen(first_id)
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.insertNode(*first)
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
		t.Fatal("Route returned nil Node.")
	}
	if !r3.ID.Equals(first_id) {
		t.Fatalf("Expected Node %s, got Node %s instead.", first_id, r3.ID)
	}
}

// Test routing to a direct match in the routing table
func TestRoutingTableRouteMatch(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := table.insertNode(*first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	if !message_id.Equals(first_id) {
		t.Fatalf("Expected ID of %s, got %s instead.", first_id, message_id)
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

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		seed := strconv.FormatInt(time.Now().UnixNano()*int64(b.N), 10)
		other_id, err := NodeIDFromBytes([]byte(seed + seed + seed))
		if err != nil {
			b.Fatalf(err.Error())
		}
		other := *NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
		b.StartTimer()
		_, err = table.insertNode(other)
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

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	seed := strconv.FormatInt(time.Now().UnixNano(), 10)
	other_id, err := NodeIDFromBytes([]byte(seed + seed + seed))
	if err != nil {
		b.Fatalf(err.Error())
	}
	other := *NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	_, err = table.insertNode(other)
	if err != nil {
		b.Fatalf(err.Error())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		table.getNode(other.ID)
	}
}

// How fast can we route messages
func BenchmarkRoutingTableRoute(b *testing.B) {
	b.StopTimer()
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	var wg sync.WaitGroup
	for i := 0; i < 100000; i = i + 1 {
		wg.Add(1)
		go func() {
			idBytes := make([]byte, 16)
			_, err := io.ReadFull(rand.Reader, idBytes)
			if err != nil {
				b.Fatalf(err.Error())
			}
			id, err := NodeIDFromBytes(idBytes)
			if err != nil {
				b.Fatalf(err.Error())
			}
			node := NewNode(id, "127.0.0.1", "127.0.0.1", "testing", 55555)
			_, err = table.insertNode(*node)
			if err != nil {
				b.Fatalf(err.Error())
			}
			wg.Done()
		}()
	}
	wg.Wait()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		idBytes := make([]byte, 16)
		_, err := io.ReadFull(rand.Reader, idBytes)
		if err != nil {
			b.Fatalf(err.Error())
		}
		id, err := NodeIDFromBytes(idBytes)
		if err != nil {
			b.Fatalf(err.Error())
		}
		b.StartTimer()
		_, err = table.route(id)
		b.StopTimer()
		if err != nil {
			b.Fatalf(err.Error())
		}
		b.StartTimer()
	}
}

// How fast can we dump the nodes in the table
func BenchmarkRoutingTableDump(b *testing.B) {
	b.StopTimer()
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	table := newRoutingTable(self)
	go table.listen()
	defer table.stop()

	var wg sync.WaitGroup
	for i := 0; i < 100000; i = i + 1 {
		wg.Add(1)
		go func() {
			idBytes := make([]byte, 16)
			_, err := io.ReadFull(rand.Reader, idBytes)
			if err != nil {
				b.Fatalf(err.Error())
			}
			id, err := NodeIDFromBytes(idBytes)
			if err != nil {
				b.Fatalf(err.Error())
			}
			node := NewNode(id, "127.0.0.1", "127.0.0.1", "testing", 55555)
			_, err = table.insertNode(*node)
			if err != nil {
				b.Fatalf(err.Error())
			}
			wg.Done()
		}()
	}
	wg.Wait()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := table.export()
		b.StopTimer()
		if err != nil {
			b.Fatalf(err.Error())
		}
		if len(nodes) < 100000 {
			b.Fatalf("Supposed to have %d nodes, have %d instead.", 100000, len(nodes))
		}
		b.StartTimer()
	}
}
