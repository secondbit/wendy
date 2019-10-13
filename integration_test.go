package wendy

import (
	"fmt"
	"testing"
	"time"
)

func toMultiAddrString(ip string) string {
	return fmt.Sprintf("/ip4/%s", ip)
}

type forwardData struct {
	next NodeID
	msg  *Message
}

type testCallback struct {
	t           *testing.T
	onDeliver   chan Message
	onForward   chan forwardData
	onNewLeaves chan []*Node
	onNodeJoin  chan Node
	onNodeExit  chan Node
	onHeartbeat chan Node
}

func newTestCallback(t *testing.T) *testCallback {
	return &testCallback{
		t:           t,
		onDeliver:   make(chan Message, 10),
		onForward:   make(chan forwardData, 10),
		onNewLeaves: make(chan []*Node, 10),
		onNodeJoin:  make(chan Node, 10),
		onNodeExit:  make(chan Node, 10),
		onHeartbeat: make(chan Node, 10),
	}
}

func (t *testCallback) OnError(err error) {
	t.t.Fatalf(err.Error())
}

func (t *testCallback) OnDeliver(msg Message) {
	select {
	case t.onDeliver <- msg:
	default:
	}
}

func (t *testCallback) OnForward(msg *Message, next NodeID) bool {
	select {
	case t.onForward <- forwardData{next: next, msg: msg}:
	default:
	}
	return true
}

func (t *testCallback) OnNewLeaves(leaves []*Node) {
	select {
	case t.onNewLeaves <- leaves:
	default:
	}
}

func (t *testCallback) OnNodeJoin(node Node) {
	select {
	case t.onNodeJoin <- node:
	default:
	}
}

func (t *testCallback) OnNodeExit(node Node) {
	select {
	case t.onNodeExit <- node:
	default:
	}
}

func (t *testCallback) OnHeartbeat(node Node) {
	select {
	case t.onHeartbeat <- node:
	default:
	}
}

func makeCluster(idBytes string, port int) (*Cluster, error) {
	id, err := NodeIDFromBytes([]byte(idBytes))
	if err != nil {
		return nil, err
	}
	node, err := NewNode(
		id,
		toMultiAddrString("127.0.0.1"),
		toMultiAddrString("127.0.0.1"), "testing", port)
	if err != nil {
		return nil, err
	}
	cluster := NewCluster(node, nil)
	cluster.SetHeartbeatFrequency(10)
	cluster.SetNetworkTimeout(1)
	cluster.SetLogLevel(LogLevelDebug)
	return cluster, nil
}

// Test joining two nodes
func TestClusterJoinTwo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	one, err := makeCluster("this is a test Node for testing purposes only.", 55555)
	if err != nil {
		t.Fatalf(err.Error())
	}
	one.debug("One is %s", one.self.ID)
	oneCB := newTestCallback(t)
	one.RegisterCallback(oneCB)
	two, err := makeCluster("this is some other Node for testing purposes only.", 55556)
	if err != nil {
		t.Fatalf(err.Error())
	}
	two.debug("Two is %s", two.self.ID)
	twoCB := newTestCallback(t)
	two.RegisterCallback(twoCB)
	go func() {
		defer one.Kill()
		err := one.Listen()
		if err != nil {
			t.Fatalf(err.Error())
		}
	}()
	go func() {
		defer two.Kill()
		err := two.Listen()
		if err != nil {
			t.Fatalf(err.Error())
		}
	}()
	time.Sleep(2 * time.Millisecond)
	err = two.Join(one.self.IPToMultiAaddr(true))
	if err != nil {
		t.Fatalf(err.Error())
	}
	ticker := time.NewTicker(3 * time.Duration(one.getNetworkTimeout()) * time.Second)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		t.Fatalf("Timeout waiting on join. Waited %d seconds.", 3*one.getNetworkTimeout())
		return
	case <-oneCB.onNodeJoin:
		_, err = one.table.getNode(two.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = two.table.getNode(one.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = one.leafset.getNode(two.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = two.leafset.getNode(one.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = one.neighborhoodset.getNode(two.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = two.neighborhoodset.getNode(one.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}
	ticker.Stop()
}

// Test joining three nodes
func TestClusterJoinThreeToTwo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	one, err := makeCluster("A test Node for testing purposes only.", 55555)
	if err != nil {
		t.Fatalf(err.Error())
	}
	one.debug("One is %s", one.self.ID)
	oneCB := newTestCallback(t)
	one.RegisterCallback(oneCB)
	two, err := makeCluster("just some other Node for testing purposes only.", 55556)
	if err != nil {
		t.Fatalf(err.Error())
	}
	two.debug("Two is %s", two.self.ID)
	twoCB := newTestCallback(t)
	two.RegisterCallback(twoCB)
	three, err := makeCluster("yet a third Node for testing purposes only.", 55557)
	if err != nil {
		t.Fatalf(err.Error())
	}
	three.debug("Three is %s", three.self.ID)
	threeCB := newTestCallback(t)
	three.RegisterCallback(threeCB)
	go func() {
		defer one.Kill()
		err := one.Listen()
		if err != nil {
			t.Fatalf(err.Error())
		}
	}()
	go func() {
		defer two.Kill()
		err := two.Listen()
		if err != nil {
			t.Fatalf(err.Error())
		}
	}()
	go func() {
		defer three.Kill()
		err := three.Listen()
		if err != nil {
			t.Fatalf(err.Error())
		}
	}()
	time.Sleep(2 * time.Millisecond)
	err = two.Join(one.self.IPToMultiAaddr(true))
	if err != nil {
		t.Fatalf(err.Error())
	}
	ticker := time.NewTicker(5 * time.Duration(one.getNetworkTimeout()) * time.Second)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		t.Fatalf("Timeout waiting on two join. Waited %d seconds.", 5*one.getNetworkTimeout())
		return
	case <-oneCB.onNodeJoin:
		_, err = one.table.getNode(two.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = two.table.getNode(one.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = one.leafset.getNode(two.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = two.leafset.getNode(one.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = one.neighborhoodset.getNode(two.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
		_, err = two.neighborhoodset.getNode(one.self.ID)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}
	ticker.Stop()
	err = three.Join(two.self.IPToMultiAaddr(true))
	if err != nil {
		t.Fatalf(err.Error())
	}
	returns := 0
	ticker = time.NewTicker(120 * time.Second)
	defer ticker.Stop()
L:
	for {
		select {
		case <-ticker.C:
			t.Fatalf("Timeout waiting on three to join. Waited %d seconds.", 120)
			return
		case <-twoCB.onNodeJoin:
			t.Logf("Got node join callback from twoCB")
			if returns < 1 {
				t.Logf("Waiting on first Node join callback")
				returns = returns + 1
				continue
			}
			break L
		case <-oneCB.onNodeJoin:
			t.Logf("Got Node join callback from oneCB")
			if returns < 1 {
				t.Logf("Waiting on second Node join callback")
				returns = returns + 1
				continue
			}
			break L
		}
	}
	_, err = one.table.getNode(three.self.ID)
	if err != nil {
		t.Logf("Error getting three from one's table")
		t.Errorf(err.Error())
	}
	_, err = two.table.getNode(three.self.ID)
	if err != nil {
		t.Logf("Error getting three from two's table")
		t.Errorf(err.Error())
	}
	_, err = three.table.getNode(one.self.ID)
	if err != nil {
		t.Logf("Error getting one from three's table")
		t.Errorf(err.Error())
	}
	_, err = three.table.getNode(two.self.ID)
	if err != nil {
		t.Logf("Error getting two from three's table")
		t.Errorf(err.Error())
	}
	_, err = one.leafset.getNode(three.self.ID)
	if err != nil {
		t.Logf("Error getting three from one's leaf set")
		t.Errorf(err.Error())
	}
	_, err = two.leafset.getNode(three.self.ID)
	if err != nil {
		t.Logf("Error getting three from two's leaf set")
		t.Errorf(err.Error())
	}
	_, err = three.leafset.getNode(one.self.ID)
	if err != nil {
		t.Logf("Error getting one from three's leaf set")
		t.Errorf(err.Error())
	}
	_, err = three.leafset.getNode(two.self.ID)
	if err != nil {
		t.Logf("Error getting two from three's leaf set")
		t.Errorf(err.Error())
	}
	_, err = one.neighborhoodset.getNode(three.self.ID)
	if err != nil {
		t.Logf("Error getting three from one's neighborhood set")
		t.Errorf(err.Error())
	}
	_, err = two.neighborhoodset.getNode(three.self.ID)
	if err != nil {
		t.Logf("Error getting three from two's neighborhood set")
		t.Errorf(err.Error())
	}
	_, err = three.neighborhoodset.getNode(one.self.ID)
	if err != nil {
		t.Logf("Error getting one from three's neighborhood set")
		t.Errorf(err.Error())
	}
	_, err = three.neighborhoodset.getNode(two.self.ID)
	if err != nil {
		t.Logf("Error getting two from three's neighborhood set")
		t.Errorf(err.Error())
	}
	return
}
