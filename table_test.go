package wendy

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/multiformats/go-multiaddr"
)

func newMultiaddr(t *testing.T, ip string, port int) multiaddr.Multiaddr {
	new := fmt.Sprintf("/ip4/%s/tcp/%v", ip, port)
	addr, err := multiaddr.NewMultiaddr(new)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

// Test insertion of a node into the routing table
func TestRoutingTableInsert(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self, err := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s\n", self_id.String())

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other, err := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	row := self_id.CommonPrefixLen(other_id)
	col := other_id.Digit(row)
	t.Logf("%s\n", other_id.String())
	t.Logf("%v\n", row)
	t.Logf("%v\n", int(col))
	table := newRoutingTable(self)
	r, err := table.insertNode(*other, self.Proximity(other))
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

// Test deleting the only node from column of the routing table
func TestRoutingTableDeleteOnly(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self, err := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other, err := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	table := newRoutingTable(self)
	r, err := table.insertNode(*other, self.Proximity(other))
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

// Test routing when the key falls in between two nodes
func TestRoutingTableScanSplit(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self, err := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	table := newRoutingTable(self)

	first_id, err := NodeIDFromBytes([]byte("12345677890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first, err := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	r, err := table.insertNode(*first, self.Proximity(first))
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
	second, err := NewNode(second_id, "127.0.0.3", "127.0.0.3", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := table.insertNode(*second, self.Proximity(second))
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
	self, err := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	table := newRoutingTable(self)

	first_id, err := NodeIDFromBytes([]byte("12345657890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	row := self_id.CommonPrefixLen(first_id)
	first, err := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	r, err := table.insertNode(*first, self.Proximity(first))
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

// Test routing over multiple rows in the routing table
func TestRoutingTableScanMultipleRows(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self, err := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	table := newRoutingTable(self)

	first_id, err := NodeIDFromBytes([]byte("1234567890abdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first, err := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	r, err := table.insertNode(*first, self.Proximity(first))
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
	second, err := NewNode(second_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := table.insertNode(*second, self.Proximity(second))
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
	self, err := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	table := newRoutingTable(self)

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	row := self_id.CommonPrefixLen(first_id)
	first, err := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	r, err := table.insertNode(*first, self.Proximity(first))
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
	self, err := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	table := newRoutingTable(self)

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first, err := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		t.Fatal(err)
	}
	r, err := table.insertNode(*first, self.Proximity(first))
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

// seed used for random number generator in all benchmarks
const randSeed = 42

var benchRand = rand.New(rand.NewSource(0))

func randomNodeID() NodeID {
	r := benchRand
	lo := uint64(r.Uint32())<<32 | uint64(r.Uint32())
	hi := uint64(r.Uint32())<<32 | uint64(r.Uint32())
	return NodeID{lo, hi}
}

// How fast can we insert nodes
func BenchmarkRoutingTableInsert(b *testing.B) {
	b.StopTimer()
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self, err := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		b.Fatal(err)
	}
	table := newRoutingTable(self)
	benchRand.Seed(randSeed)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		otherId := randomNodeID()
		other, err := NewNode(otherId, "127.0.0.2", "127.0.0.2", "testing", 55555)
		if err != nil {
			b.Fatal(err)
		}
		_, err = table.insertNode(*other, self.Proximity(other))
	}
}

// How fast can we retrieve nodes by ID
func BenchmarkRoutingTableGetByID(b *testing.B) {
	b.StopTimer()
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self, err := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		b.Fatal(err)
	}
	table := newRoutingTable(self)
	benchRand.Seed(randSeed)

	otherId := randomNodeID()
	other, err := NewNode(otherId, "127.0.0.2", "127.0.0.2", "testing", 55555)
	if err != nil {
		b.Fatal(err)
	}
	_, err = table.insertNode(*other, self.Proximity(other))
	if err != nil {
		b.Fatalf(err.Error())
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		table.getNode(other.ID)
	}
}

var benchTable *routingTable

func initBenchTable(b *testing.B) {
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self, err := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)
	if err != nil {
		b.Fatal(err)
	}
	benchTable = newRoutingTable(self)
	benchRand.Seed(randSeed)

	for i := 0; i < 100000; i++ {
		id := randomNodeID()
		node, err := NewNode(id, "127.0.0.1", "127.0.0.1", "testing", 55555)
		if err != nil {
			b.Fatal(err)
		}
		_, err = benchTable.insertNode(*node, self.Proximity(node))
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}

// How fast can we route messages
func BenchmarkRoutingTableRoute(b *testing.B) {
	b.StopTimer()
	if benchTable == nil {
		initBenchTable(b)
	}
	benchRand.Seed(randSeed)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		id := randomNodeID()
		_, err := benchTable.route(id)
		if err != nil && err != nodeNotFoundError {
			b.Fatalf(err.Error())
		}
	}
}

// How fast can we dump the nodes in the table
func BenchmarkRoutingTableDump(b *testing.B) {
	b.StopTimer()
	if benchTable == nil {
		initBenchTable(b)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchTable.list([]int{}, []int{})
	}
}

func BenchmarkRoutingTableDumpPartial(b *testing.B) {
	b.StopTimer()
	if benchTable == nil {
		initBenchTable(b)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchTable.list([]int{0, 1, 2, 3, 4, 5, 6}, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	}
}

func BenchmarkRoutingTableExport(b *testing.B) {
	b.StopTimer()
	if benchTable == nil {
		initBenchTable(b)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchTable.export([]int{}, []int{})
	}
}

func BenchmarkRoutingTableExportPartial(b *testing.B) {
	b.StopTimer()
	if benchTable == nil {
		initBenchTable(b)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchTable.export([]int{0, 1, 2, 3, 4, 5, 6}, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	}
}
