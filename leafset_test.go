package pastry

import (
	"testing"
)

// Test insertion of a node into the routing table
func TestLeafSetInsert(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	if r.Pos != 0 {
		t.Fatalf("Expected node to be in pos %d, was put in %d instead.", 0, r.Pos)
	}
	side := self_id.RelPos(other_id)
	side_str := "left"
	other_side_str := "right"
	if side == 1 {
		side_str = "right"
		other_side_str = "left"
	}
	if (r.Left && side == 1) || (!r.Left && side == -1) {
		t.Fatalf("Expected node to be to the %s, was to the %s instead.", side_str, other_side_str)
	}
	r2, err := leafset.Get(nil, 0, r.Left)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r2 == nil {
		t.Fatalf("Nil response returned.")
	}
	if r2.Node == nil {
		t.Fatalf("Expected node, got nil instead.")
	}
	if !r2.Node.ID.Equals(other_id) {
		t.Fatalf("Expected Node %s, got Node %s instead.", other_id, r2.Node.ID)
	}
}

// Test handling of a Node being inserted twice.
func TestLeafSetDoubleInsert(t *testing.T) {
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
	t.Log(other)
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("First insert returned a nil response.")
	}
	r2, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r2 == nil {
		t.Fatalf("Second insert returned a nil response.")
	}
	if r.Pos != r2.Pos {
		t.Errorf("Positions expected to be equal. %d != %d", r.Pos, r2.Pos)
	}
}

// Test retrieving a Node by ID
func TestLeafSetGetByID(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()

	other_id, err := NodeIDFromBytes([]byte("this is some other Node for testing purposes only."))
	if err != nil {
		t.Fatal(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil response.")
	}
	r2, err := leafset.Get(other, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Returned nil response.")
	}
	if r2.Pos != r.Pos {
		t.Errorf("Expected pos %v, got pos %v.", r.Pos, r2.Pos)
	}
	if r2.Left != r.Left {
		expectation := "left"
		result := "right"
		if !r.Left {
			expectation = "right"
			result = "left"
		}
		t.Errorf("Expected node to be on the %v, but it was inserted on the %v.", expectation, result)
	}
	if r2.Node == nil {
		t.Fatalf("r2 returned nil node")
	}
	if r.Node == nil {
		t.Fatalf("r returned nil node")
	}
	if !r2.Node.ID.Equals(r.Node.ID) {
		t.Errorf("Expected node %v, got node %v.", r.Node.ID, r2.Node.ID)
	}
}

// Test retrieving a node by position
func TestLeafSetGetByPos(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("this is a test Node for testing purposes only."))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()

	other_id, err := NodeIDFromBytes([]byte("This is another test Node for testing purposes only."))
	if err != nil {
		t.Fatal(err.Error())
	}
	other := NewNode(other_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatal(err.Error())
	}
	r2, err := leafset.Get(nil, r.Pos, r.Left)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Returned nil response.")
	}
	if r2.Node == nil {
		t.Fatalf("r2 returned nil node")
	}
	if r.Node == nil {
		t.Fatalf("r returned nil node")
	}
	if !r2.Node.ID.Equals(r.Node.ID) {
		t.Errorf("Expected node %v, got node %v.", r.Node.ID, r2.Node.ID)
	}
}

// Test deleting the only node from the leafset using its position
func TestLeafSetDeleteOnlyByPos(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	_, err = leafset.Remove(nil, r.Pos, r.Left)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := leafset.Get(nil, r.Pos, r.Left)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil && r3.Node != nil && r3.Node.ID.Equals(other_id) {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
}

// Test deleting the only node from the leafset using its ID
func TestLeafSetDeleteOnlyByID(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	_, err = leafset.Remove(r.Node, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := leafset.Get(r.Node, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil && r3.Node != nil && r3.Node.ID.Equals(other_id) {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
}

// Test deleting the first of two nodes from the leafset using its position
func TestLeafSetDeleteFirstByPos(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Left != r2.Left {
		expectation := "left"
		result := "right"
		if !r.Left {
			expectation = "right"
			result = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Expected %v, got %v.", expectation, result)
	}
	if r2.Pos != 1 {
		t.Fatalf("Second insert didn't get added to the end of the column. Expected 1, got %v.", r2.Pos)
	}
	var removed *leafSetRequest
	removed, err = leafset.Remove(nil, 0, r.Left)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if removed == nil {
		t.Fatalf("Returned nil response.")
	}
	r3, err := leafset.Get(removed.Node, -1, r.Left)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil && r3.Node != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r4, err := leafset.Get(nil, 0, r.Left)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for second insert.")
	}
	if r4.Node == nil {
		t.Fatalf("Got nil node when querying for second insert.")
	}
}

// Test deleting the first of two nodes from the leafset using its ID
func TestLeafSetDeleteFirstByID(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Left != r2.Left {
		expectation := "left"
		result := "right"
		if !r.Left {
			expectation = "right"
			result = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Expected %v, got %v.", expectation, result)
	}
	var firstnode *Node
	var secondnode *Node
	if r.Pos < r2.Pos {
		firstnode = r.Node
		secondnode = r2.Node
	} else {
		firstnode = r2.Node
		secondnode = r.Node
	}
	_, err = leafset.Remove(firstnode, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := leafset.Get(firstnode, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r4, err := leafset.Get(secondnode, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for second insert.")
	}
	if r4.Pos != 0 {
		t.Errorf("Expected second insert to be in position 0, got %v instead.", r4.Pos)
	}
}

// Test deleting the last of multiple nodes from the leafset using its position
func TestLeafSetDeleteLastByPos(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Left != r2.Left {
		expected := "left"
		reality := "right"
		if !r.Left {
			expected = "right"
			reality = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Expected %v, got %v.", expected, reality)
	}
	var firstnode *Node
	var secondnode *Node
	if r.Pos < r2.Pos {
		firstnode = r.Node
		secondnode = r2.Node
	} else {
		firstnode = r2.Node
		secondnode = r.Node
	}
	_, err = leafset.Remove(nil, 1, r.Left)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := leafset.Get(firstnode, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 == nil {
		t.Fatalf("Expected node, got nil response.")
	}
	if r3.Node == nil {
		t.Fatalf("Nil node returned.")
	}
	if !firstnode.ID.Equals(r3.Node.ID) {
		t.Errorf("Expected node %s, got node %s", secondnode.ID, r3.Node.ID)
	}
	if r3.Pos != 0 {
		t.Errorf("Expected first insert to be in position 0, got %v instead.", r3.Pos)
	}
	r4, err := leafset.Get(secondnode, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 != nil {
		t.Fatalf("Expected nil response when querying for first insert, got %v instead.", r4.Node)
	}
}

// Test deleting the last of multiple nodes from the leafset based on its ID
func TestLeafSetDeleteLastByID(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Left != r2.Left {
		expected := "left"
		reality := "right"
		if !r.Left {
			expected = "right"
			reality = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Expected the second node to be on the %s, was on the %s instead.", expected, reality)
	}
	var firstnode, secondnode *Node
	if r.Pos < r2.Pos {
		firstnode = r.Node
		secondnode = r2.Node
	} else {
		firstnode = r2.Node
		secondnode = r.Node
	}
	_, err = leafset.Remove(secondnode, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r3, err := leafset.Get(secondnode, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r3 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r3.Node.ID)
	}
	r4, err := leafset.Get(firstnode, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r4 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	if r4.Pos != 0 {
		t.Errorf("Expected first insert to be in position 0, got %v instead.", r4.Pos)
	}
}

// Test deleting the middle of multiple nodes from the leafset using its position
func TestLeafSetDeleteMiddleByPos(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Left != r2.Left {
		expected := "left"
		reality := "right"
		if !r.Left {
			expected = "right"
			reality = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Expected the second node to be on the %s, was on the %s instead.", expected, reality)
	}
	r3, err := leafset.Insert(third)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Left != r3.Left {
		expected := "left"
		reality := "right"
		if !r.Left {
			expected = "right"
			reality = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Expected the third node to be on the %s, was on the %s instead.", expected, reality)
	}
	var req1, req2, req3 *leafSetRequest
	if r3.Pos == 0 {
		req1 = r3
		if r2.Pos == 0 {
			req2 = r2
			req3 = r
		} else {
			req2 = r
			req3 = r2
		}
	} else if r3.Pos == 1 {
		req2 = r3
		if r2.Pos == 0 {
			req1 = r2
			req3 = r
		} else {
			req1 = r
			req3 = r2
		}
	} else {
		req3 = r3
		if r2.Pos == 0 {
			req1 = r2
			req2 = r
		} else {
			req1 = r
			req2 = r2
		}
	}
	_, err = leafset.Remove(nil, 1, req2.Left)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r4, err := leafset.Get(req2.Node, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r4 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r4.Node.ID)
	}
	r5, err := leafset.Get(req1.Node, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r5 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	if r5.Pos != 0 {
		t.Errorf("Expected first insert to be in position 0, got %v instead.", r5.Pos)
	}
	r6, err := leafset.Get(req3.Node, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r6 == nil {
		t.Fatalf("Got nil response when querying for third insert.")
	}
	if r6.Pos != 1 {
		t.Errorf("Expected third insert to be in position 1, got %v instead.", r6.Pos)
	}
}

// Test deleting the middle of multiple nodes from the leafset using its ID
func TestLeafSetDeleteMiddleByID(t *testing.T) {
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
	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()
	r, err := leafset.Insert(other)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r == nil {
		t.Fatalf("Nil response returned.")
	}
	r2, err := leafset.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Left != r2.Left {
		expected := "left"
		reality := "right"
		if !r.Left {
			expected = "right"
			reality = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Expected the second node to be on the %s, was on the %s instead.", expected, reality)
	}
	r3, err := leafset.Insert(third)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatal("Nil response returned.")
	}
	if r.Left != r3.Left {
		expected := "left"
		reality := "right"
		if !r.Left {
			expected = "right"
			reality = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Expected the third node to be on the %s, was on the %s instead.", expected, reality)
	}
	var req1, req2, req3 *leafSetRequest
	if r3.Pos == 0 {
		req1 = r3
		if r2.Pos == 0 {
			req2 = r2
			req3 = r
		} else {
			req2 = r
			req3 = r2
		}
	} else if r3.Pos == 1 {
		req2 = r3
		if r2.Pos == 0 {
			req1 = r2
			req3 = r
		} else {
			req1 = r
			req3 = r2
		}
	} else {
		req3 = r3
		if r2.Pos == 0 {
			req1 = r2
			req2 = r
		} else {
			req1 = r
			req2 = r2
		}
	}
	_, err = leafset.Remove(req2.Node, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r4, err := leafset.Get(req2.Node, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if r4 != nil {
		t.Errorf("Expected nil response, got Node %s instead.", r4.Node.ID)
	}
	r5, err := leafset.Get(req1.Node, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r5 == nil {
		t.Fatalf("Got nil response when querying for first insert.")
	}
	if r5.Pos != 0 {
		t.Errorf("Expected first insert to be in position 0, got %v instead.", r5.Pos)
	}
	r6, err := leafset.Get(req3.Node, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r6 == nil {
		t.Fatalf("Got nil response when querying for third insert.")
	}
	if r6.Pos != 1 {
		t.Errorf("Expected third insert to be in position 1, got %v instead.", r6.Pos)
	}
}

// Test scanning the leafset when the key falls in between two nodes
func TestLeafSetScanSplit(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234560890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()

	first_id, err := NodeIDFromBytes([]byte("12345677890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.Insert(first)
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
	r2, err := leafset.Insert(second)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r2 == nil {
		t.Fatal("Second insert returned nil")
	}
	if r.Left != r2.Left {
		expected := "left"
		reality := "right"
		if !r.Left {
			expected = "right"
			reality = "left"
		}
		t.Fatalf("Nodes not inserted on the same side. Second node was supposed to be inserted on the %s, got inserted on the %s.", expected, reality)
	}
	message_id, err := NodeIDFromBytes([]byte("12345657890abcde"))
	if err != nil {
		t.Fatal(err.Error())
	}
	msg_side := message_id.RelPos(r.Node.ID) 
	if msg_side == -1 && !r.Left {
		t.Fatalf("Message would be on left, nodes are on right.")
	} else if msg_side == 1 && r.Left {
		t.Fatalf("Message would be on right, nodes are on left.")
	}
	d1 := message_id.Diff(first_id)
	d2 := message_id.Diff(second_id)
	if d1.Cmp(d2) != 0 {
		t.Fatalf("IDs not equidistant. Expected %s, got %s.", d1, d2)
	}
	if !second_id.Less(first_id) {
		t.Fatalf("Second ID is not lower than the first ID.")
	}
	if !leafset.contains(message_id) {
		t.Fatalf("Message ID not contained in leaf set.")
	}
	r3, err := leafset.Scan(message_id)
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

// Test scanning the leafset when there are no suitable matches
func TestLeafSetScanNone(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdef"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.Insert(first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890abcdeh"))
	if err != nil {
		t.Fatal(err.Error())
	}
	if leafset.contains(message_id) {
		t.Fatalf("Node would be picked up by scan.")
	}
	r3, err := leafset.Scan(message_id)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r3 == nil {
		t.Fatalf("Scan should return last node on the side, returned nil instead.")
	}
	var side [16]*Node
	if r.Left {
		side = leafset.left
	} else {
		side = leafset.right
	}
	lastnode := lastNode(side)
	if lastnode == nil {
		t.Fatalf("lastNode returned nil.")
	}
	if !r3.Node.ID.Equals(lastnode.ID) {
		t.Errorf("Scan was supposed to return %s, returned %s instead.", lastnode.ID, r3.Node.ID)
	}
}

// Test routing to the only node in the leafset
func TestLeafSetRouteOnly(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.Insert(first)
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
	side := self.ID.RelPos(message_id)
	if side == 1 && r.Left {
		t.Fatalf("Message would be on the right, node is on the left.")
	} else if side == -1 && !r.Left {
		t.Fatalf("Message would be on the left, node is on the right.")
	}
	r3, err := leafset.route(message_id)
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

// Test routing to a direct match in the leafset
func TestLeafSetRouteMatch(t *testing.T) {
	self_id, err := NodeIDFromBytes([]byte("1234567890abcdeg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	self := NewNode(self_id, "127.0.0.1", "127.0.0.1", "testing", 55555)

	leafset := NewRoutingTable(self)
	go leafset.listen()
	defer leafset.Stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890acdefg"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.Insert(first)
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
	if r3 == nil {
		t.Fatal("Route returned nil Node.")
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

	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890abcdeh"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.Insert(first)
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
	if leafset.contains(message_id) {
		t.Fatalf("Message contained in leaf set.")
	}
	r3, err := leafset.route(message_id)
	if err != nil {
		t.Fatal(err.Error())
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

	leafset := NewLeafSet(self)
	go leafset.listen()
	defer leafset.Stop()

	first_id, err := NodeIDFromBytes([]byte("1234567890abcdea"))
	if err != nil {
		t.Fatal(err.Error())
	}
	first := NewNode(first_id, "127.0.0.2", "127.0.0.2", "testing", 55555)
	r, err := leafset.Insert(first)
	if err != nil {
		t.Fatal(err.Error())
	}
	if r == nil {
		t.Fatal("Insert returned nil.")
	}
	message_id, err := NodeIDFromBytes([]byte("1234567890abcded"))
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
		t.Fatal(err.Error())
	}
	if r3 != nil {
		t.Fatalf("Expected nil result, got %s instead.", r3.ID)
	}
}
