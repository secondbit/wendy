package wendy

import (
	"testing"
)

// Test insertion of a node into the neighborhood set
func TestNeighborhoodSetInsertNode(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is just a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 0)
	t.Logf("%s\n", self_id.String())

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 0)
	t.Logf("%s\n", other_id.String())
	neighborhood := newNeighborhoodSet(self)
	r, err := neighborhood.insertNode(*other, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := neighborhood.getNode(other_id)
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

// Test deleting the only node from the neighborhood set
func TestNeighborhoodSetDeleteOnly(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is just a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 0)
	t.Logf("%s\n", self_id.String())

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 0)
	t.Logf("%s\n", other_id.String())
	neighborhood := newNeighborhoodSet(self)
	r, err := neighborhood.insertNode(*other, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	_, err = neighborhood.removeNode(other_id)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := neighborhood.getNode(other_id)
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

/// Test deleting the first of two nodes from the neighborhood set
func TestNeighborhoodSetDeleteFirst(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is just a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 0)
	t.Logf("%s\n", self_id.String())

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 0)
	t.Logf("%s\n", other_id.String())
	neighborhood := newNeighborhoodSet(self)
	r, err := neighborhood.insertNode(*other, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	second_id, err := NodeIDFromBytes([]byte("just a third Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "!27.0.0.3", "127.0.0.3", "testing", 0)
	r = nil
	r, err = neighborhood.insertNode(*second, 10)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Nil response returned")
	}
	_, err = neighborhood.removeNode(other_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	r = nil
	r, err = neighborhood.getNode(other_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatal(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r.ID)
	}
	r = nil
	r, err = neighborhood.getNode(second_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatalf("Got nil response when I expected to get Node %s", second_id)
	}
	if !r.ID.Equals(second_id) {
		t.Fatalf("Expected %s, got %s.", second_id, r.ID)
	}
}

/// Test deleting the last of two nodes from the neighborhood set
func TestNeighborhoodSetDeleteLast(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is just a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 0)
	t.Logf("%s\n", self_id.String())

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 0)
	t.Logf("%s\n", other_id.String())
	neighborhood := newNeighborhoodSet(self)
	r, err := neighborhood.insertNode(*other, 10)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	second_id, err := NodeIDFromBytes([]byte("just a third Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "!27.0.0.3", "127.0.0.3", "testing", 0)
	r = nil
	r, err = neighborhood.insertNode(*second, 0)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Nil response returned")
	}
	_, err = neighborhood.removeNode(other_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	r = nil
	r, err = neighborhood.getNode(other_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatal(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r.ID)
	}
	r = nil
	r, err = neighborhood.getNode(second_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatalf("Got nil response when I expected to get Node %s", second_id)
	}
	if !r.ID.Equals(second_id) {
		t.Fatalf("Expected %s, got %s.", second_id, r.ID)
	}
}

/// Test deleting the middle of three nodes from the neighborhood set
func TestNeighborhoodSetDeleteMiddle(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is just a test Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 0)
	t.Logf("%s\n", self_id.String())

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 0)
	t.Logf("%s\n", other_id.String())
	neighborhood := newNeighborhoodSet(self)
	r, err := neighborhood.insertNode(*other, 0)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	second_id, err := NodeIDFromBytes([]byte("just a third Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	second := NewNode(second_id, "!27.0.0.3", "127.0.0.3", "testing", 0)
	r = nil
	r, err = neighborhood.insertNode(*second, 10)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Nil response returned")
	}
	third_id, err := NodeIDFromBytes([]byte("just a fourth Node for testing purposes only."))
	if err != nil {
		t.Fatalf(err.Error())
	}
	third := NewNode(third_id, "127.0.0.4", "127.0.0.4", "testing", 0)
	r = nil
	r, err = neighborhood.insertNode(*third, 20)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Nil response returned")
	}
	_, err = neighborhood.removeNode(second_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	r = nil
	r, err = neighborhood.getNode(second_id)
	if err != nodeNotFoundError {
		if err != nil {
			t.Fatal(err.Error())
		} else {
			t.Fatal("Expected nodeNotFoundError, got nil error instead.")
		}
	}
	if r != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r.ID)
	}
	r = nil
	r, err = neighborhood.getNode(other_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Got nil response when querying for first insert.")
	}
	r = nil
	r, err = neighborhood.getNode(third_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Got nil response when querying for third insert.")
	}
}

//////////////////////////////////////////////////////////////////////////
////////////////////////// Benchmarks ////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

// How fast can we insert nodes
func BenchmarkNeighborhoodSetInsert(b *testing.B) {
	b.StopTimer()
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)

	neighborhood := newNeighborhoodSet(self)
	benchRand.Seed(randSeed)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		otherId := randomNodeID()
		other := *NewNode(otherId, "127.0.0.1", "127.0.0.2", "testing", 55555)
		_, err = neighborhood.insertNode(other, int64(i%len(neighborhood.nodes)))
	}
}

// How fast can we retrieve nodes by ID
func BenchmarkNeighborhoodSetGetByID(b *testing.B) {
	b.StopTimer()
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)

	neighborhood := newNeighborhoodSet(self)
	benchRand.Seed(randSeed)

	for i := 0; i < len(neighborhood.nodes); i++ {
		otherId := randomNodeID()
		other := *NewNode(otherId, "127.0.0.2", "127.0.0.2", "testing", 55555)
		_, err = neighborhood.insertNode(other, int64(i))
		if err != nil {
			b.Fatalf(err.Error())
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		neighborhood.getNode(neighborhood.nodes[i%len(neighborhood.nodes)].ID)
	}
}

var benchNeighborhood *neighborhoodSet

func initBenchNeighborhoodSet(b *testing.B) {
	selfId, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		b.Fatalf(err.Error())
	}
	self := NewNode(selfId, "127.0.0.1", "127.0.0.1", "testing", 55555)
	benchNeighborhood = newNeighborhoodSet(self)
	benchRand.Seed(randSeed)

	for i := 0; i < len(benchNeighborhood.nodes); i++ {
		id := randomNodeID()
		node := NewNode(id, "127.0.0.1", "127.0.0.1", "testing", 55555)
		_, err = benchNeighborhood.insertNode(*node, int64(i))
		if err != nil {
			b.Fatal(err.Error())
		}
	}
}

// How fast can we dump the neighborhood set
func BenchmarkNeighborhoodSetDump(b *testing.B) {
	b.StopTimer()
	if benchNeighborhood == nil {
		initBenchNeighborhoodSet(b)
	}
	benchRand.Seed(randSeed)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchNeighborhood.list()
	}
}

// How fast can we export the neighborhood set
func BenchmarkNeighborhoodSetExport(b *testing.B) {
	b.StopTimer()
	if benchNeighborhood == nil {
		initBenchNeighborhoodSet(b)
	}
	benchRand.Seed(randSeed)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		benchNeighborhood.export()
	}
}
