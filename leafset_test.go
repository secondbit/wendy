package wendy

import (
	"testing"
)

// Test insertion of a node into the leaf set
func TestLeafSetinsertNode(t *testing.T) {
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
	t.Logf("%s\n", other_id.String())
	t.Logf("Diff: %v\n", self_id.Diff(other_id))
	leafset := newLeafSet(self)
	r, err := leafset.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.getNode(other_id)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r2 == nil {
		t.Fatalf("Nil response returned.")
	}
	if !r2.ID.Equals(other_id) {
		t.Fatalf("Expected Node %s, got Node %s instead.", other_id, r2.ID)
	}
}

// Test deleting the only node from the leafset
func TestLeafSetDeleteOnly(t *testing.T) {
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
	leafset := newLeafSet(self)
	r, err := leafset.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	_, err = leafset.removeNode(other_id)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := leafset.getNode(other_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatalf(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error.")
		}
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.ID)
	}
}

// Test deleting the first of two nodes from the leafset
func TestLeafSetDeleteFirst(t *testing.T) {
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
	first_side := self.ID.RelPos(other_id)
	second_side := self.ID.RelPos(second_id)
	if first_side != second_side {
		t.Fatalf("Expected %v, got %v.", first_side, second_side)
	}
	leafset := newLeafSet(self)
	r, err := leafset.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	var firstnode, secondnode *Node
	first_dist := self.ID.Diff(other_id)
	second_dist := self.ID.Diff(second_id)
	if first_dist.Cmp(second_dist) < 0 {
		firstnode = r
		secondnode = r2
	} else {
		secondnode = r
		firstnode = r2
	}
	_, err = leafset.removeNode(firstnode.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := leafset.getNode(firstnode.ID)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatalf(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.ID)
	}
	r4, err := leafset.getNode(secondnode.ID)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for second insert.")
	}
}

// Test deleting the last of multiple nodes from the leafset
func TestLeafSetDeleteLast(t *testing.T) {
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
	first_side := self.ID.RelPos(other_id)
	second_side := self.ID.RelPos(second_id)
	if first_side != second_side {
		t.Fatalf("Expected %v, got %v.", first_side, second_side)
	}
	leafset := newLeafSet(self)
	r, err := leafset.insertNode(*other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	var firstnode, secondnode *Node
	first_dist := self.ID.Diff(other_id)
	second_dist := self.ID.Diff(second_id)
	if first_dist.Cmp(second_dist) < 0 {
		firstnode = r
		secondnode = r2
	} else {
		secondnode = r
		firstnode = r2
	}
	_, err = leafset.removeNode(secondnode.ID)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := leafset.getNode(secondnode.ID)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatalf(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.ID)
	}
	r4, err := leafset.getNode(firstnode.ID)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
}

// Test deleting the middle of multiple nodes from the leafset
func TestLeafSetDeleteMiddle(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	first_id, err := NodeIDFromBytes([]byte("1234557890abcdef"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
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
	first_side := self.ID.RelPos(first_id)
	second_side := self.ID.RelPos(second_id)
	third_side := self.ID.RelPos(third_id)
	if first_side != second_side || second_side != third_side {
		t.Fatalf("Nodes not all on same side. %v, %v, %v", first_side, second_side, third_side)
	}
	leafset := newLeafSet(self)
	r, err := leafset.insertNode(*first)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	r3, err := leafset.insertNode(*third)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Nil response returned.")
	}
	var zero, one, two NodeID
	first_dist := self.ID.Diff(first_id)
	second_dist := self.ID.Diff(second_id)
	third_dist := self.ID.Diff(third_id)
	if first_dist.Cmp(second_dist) < 0 && first_dist.Cmp(third_dist) < 0 {
		zero = first_id
		if second_dist.Cmp(third_dist) < 0 {
			one = second_id
			two = third_id
		} else {
			one = third_id
			two = second_id
		}
	} else if first_dist.Cmp(second_dist) < 0 && first_dist.Cmp(third_dist) > 0 {
		zero = third_id
		one = first_id
		two = second_id
	} else if first_dist.Cmp(second_dist) > 0 && first_dist.Cmp(third_dist) < 0 {
		zero = second_id
		one = first_id
		two = third_id
	} else {
		if second_dist.Cmp(third_dist) < 0 {
			zero = second_id
			one = third_id
			two = first_id
		} else {
			zero = third_id
			one = second_id
			two = first_id
		}
	}
	r4, err := leafset.removeNode(one)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r4 == nil {
		t.Fatal("Expected node, got nil instead.")
	}
	r5, err := leafset.getNode(one)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatalf(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error.")
		}
	}
	if r5 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r5.ID)
	}
	r6, err := leafset.getNode(zero)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r6 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	r7, err := leafset.getNode(two)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r7 == nil {
		t.Fatalf("Got nil response when querying for third insert.")
	}
}

// Test scanning the leafset when the key falls in between two nodes
func TestLeafSetScanSplit(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := newLeafSet(self)

	first_id, err := NodeIDFromBytes([]byte("12345677890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.insertNode(*first)
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
	r2, err := leafset.insertNode(*second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Second insert returned nil")
	}
	first_side := self.ID.RelPos(first_id)
	second_side := self.ID.RelPos(second_id)
	if first_side != second_side {
		t.Fatalf("Nodes not inserted on the same side. %v vs. %v.", first_side, second_side)
	}
	message_id, err := NodeIDFromBytes([]byte("12345657890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	msg_side := self.ID.RelPos(message_id)
	if msg_side != first_side {
		t.Fatalf("Message not on the same side as the nodes. %v vs. %v.", msg_side, first_side)
	}
	d1 := message_id.Diff(first_id)
	d2 := message_id.Diff(second_id)
	if d1.Cmp(d2) != 0 {
		t.Fatalf("IDs not equidistant. Expected %v, got %v.", d1, d2)
	}
	if !second_id.Less(first_id) {
		t.Fatalf("%v is not lower than the %v.", second_id.Base10(), first_id.Base10())
	}
	r3, err := leafset.route(message_id)
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

// Test routing to the only node in the leafset
func TestLeafSetRouteOnly(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := newLeafSet(self)

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.insertNode(*first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890acdeff"))
	if err != nil {
		t.Fatal(err.Error())
	}
	msg_side := self.ID.RelPos(message_id)
	first_side := self.ID.RelPos(first_id)
	if msg_side != first_side {
		t.Fatalf("Message and node not on same side. %v vs. %v.", msg_side, first_side)
	}
	r3, err := leafset.route(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Route returned nil.")
	}
	if !r3.ID.Equals(first_id) {
		t.Fatalf("Expected Node %s, got Node %s instead.", first_id, r3.ID)
	}
}

// Test routing to a direct match in the leafset
func TestLeafSetRouteMatch(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := newLeafSet(self)

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.insertNode(*first)
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
	r3, err := leafset.route(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Route returned nil.")
	}
	if !r3.ID.Equals(first_id) {
		t.Fatalf("Expected Node %s, got Node %s instead.", first_id, r3.ID)
	}
}

// Test routing when the message is not within the leafset
func TestLeafSetRouteNoneContained(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := newLeafSet(self)

	first_id, err := NodeIDFromBytes([]byte("1234567890abcdeh"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.insertNode(*first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("123456789abcdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	r3, err := leafset.route(message_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatal(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r3 != nil {
		t.Fatalf("Expected nil result, got %s instead.", r3.ID)
	}
}

// Test routing when there are no nodes in the leafset closer than the current node
func TestLeafSetRouteNoneCloser(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := newLeafSet(self)

	first_id, err := NodeIDFromBytes([]byte("1234567890abcdez"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.insertNode(*first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self_diff := self_id.Diff(message_id)
	node_diff := first_id.Diff(message_id)
	node_closer := self_diff.Cmp(node_diff) == 1
	if node_closer {
		t.Fatalf("Node is closer.")
	}
	r3, err := leafset.route(message_id)
	if err != nil {
		if _, ok := err.(IdentityError); !ok {
			t.Fatal(err.Error())
		}
	} else {
		t.Fatal("Expected an IdentityError, but got a nil error instead.")
	}
	if r3 != nil {
		t.Fatalf("Expected nil result, got %s instead.", r3.ID)
	}
}

//////////////////////////////////////////////////////////////////////////
////////////////////////// Benchmarks ////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

// How fast can we insert nodes
func BenchmarkLeafSetInsert(b *testing.B) {
	b.StopTimer()
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := newLeafSet(self)
	benchRand.Seed(randSeed)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		otherId := randomNodeID()
		other := *NewNode(otherId, "127.0.0.1", "127.0.0.2", "testing", 55555)
		_, err = leafset.insertNode(other)
	}
}

// How fast can we retrieve nodes by ID
func BenchmarkLeafSetGetByID(b *testing.B) {
	b.StopTimer()
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := newLeafSet(self)
	benchRand.Seed(randSeed)

	otherId := randomNodeID()
	other := *NewNode(otherId, "127.0.0.2", "127.0.0.2", "testing", 55555)
	_, err = leafset.insertNode(other)
	if err != nil {
		b.Fatalf(err.Error())
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		leafset.getNode(other.ID)
	}
}

var benchLeafSet *leafSet

func initBenchLeafSet(b *testing.B) {
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)
	benchLeafSet = newLeafSet(self)
	benchRand.Seed(randSeed)

	for i := 0; i < 100000; i++ {
		id := randomNodeID()
		node := NewNode(id, "127.0.0.1", "127.0.0.1", "testing", 55555)
		_, err = benchLeafSet.insertNode(*node)
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}

// How fast can we route messages
func BenchmarkLeafSetRoute(b *testing.B) {
	b.StopTimer()
	if benchLeafSet == nil {
		initBenchLeafSet(b)
	}
	benchRand.Seed(randSeed)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		id := randomNodeID()
		_, err := benchLeafSet.route(id)
		if err != nil && err != nodeNotFoundError {
			if _, ok := err.(IdentityError); !ok {
				b.Fatalf(err.Error())
			}
		}
	}
}

// How fast can we dump the leafset
func BenchmarkLeafSetDump(b *testing.B) {
	b.StopTimer()
	if benchLeafSet == nil {
		initBenchLeafSet(b)
	}
	benchRand.Seed(randSeed)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchLeafSet.list()
	}
}

// How fast can we export the leafset
func BenchmarkLeafSetExport(b *testing.B) {
	b.StopTimer()
	if benchLeafSet == nil {
		initBenchLeafSet(b)
	}
	benchRand.Seed(randSeed)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchLeafSet.export()
	}
}
