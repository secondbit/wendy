package wendy

import (
	"bytes"
	"math/big"
	"testing"

	ci "github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func TestNodeIDString(t *testing.T) {
	tests := [...]struct {
		bytes []byte
		str   string
	}{
		{
			make([]byte, 16),
			"00000000000000000000000000000000",
		},
		{
			bytes.Repeat([]byte{0xff}, 16),
			"ffffffffffffffffffffffffffffffff",
		},
	}
	for i, test := range tests {
		id, err := NodeIDFromBytes(test.bytes)
		if err != nil {
			t.Errorf("test %v: unexpected error %v", i, err)
		}
		str := id.String()
		if str != test.str {
			t.Errorf("test %v: expected %q, got %q", i, test.str, str)
		}
	}
}

func Test_NodeIDFromPeerID(t *testing.T) {
	pk, _, err := ci.GenerateKeyPair(ci.Ed25519, 256)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := peer.IDFromPrivateKey(pk)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NodeIDFromPeerID(pid); err != nil {
		t.Fatal(err)
	}
	if _, err := NodeIDFromPeerID(peer.ID("err")); err == nil {
		t.Fatal("error expected")
	}
}

func TestNodeIDRelPos(t *testing.T) {
	tests := [...]struct {
		bytes1, bytes2 []byte
		relpos         int
	}{
		{
			make([]byte, 16),
			make([]byte, 16),
			0,
		},
		{
			make([]byte, 16),
			bytes.Repeat([]byte{0x11}, 16),
			-1,
		},
		{
			bytes.Repeat([]byte{0x11}, 16),
			make([]byte, 16),
			1,
		},
		{
			make([]byte, 16),
			bytes.Repeat([]byte{0xff}, 16),
			1,
		},
		{
			bytes.Repeat([]byte{0xff}, 16),
			make([]byte, 16),
			-1,
		},
		{
			[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xf9, 0x00, 0x00, 0xf7, 0x31, 0x01, 0x01, 0x01, 0x01, 0x01},
			[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0xff, 0xff, 0x08, 0xce, 0xfe, 0xfe, 0xfe, 0xfe, 0xff},
			-1,
		},
	}
	for i, test := range tests {
		id1, err := NodeIDFromBytes(test.bytes1)
		if err != nil {
			t.Errorf("test %v: unexpected error %v", i, err)
		}
		id2, err := NodeIDFromBytes(test.bytes2)
		if err != nil {
			t.Errorf("test %v: unexpected error %v", i, err)
		}
		relpos := id1.RelPos(id2)
		if relpos != test.relpos {
			t.Errorf("test %v: expected %v, got %v", i, test.relpos, relpos)
		}
	}
}

func TestNodeIDBase10(t *testing.T) {
	tests := [...]struct {
		bytes  []byte
		base10 *big.Int
	}{
		{
			make([]byte, 16),
			big.NewInt(0),
		},
		{
			append(make([]byte, 15), 1),
			big.NewInt(1),
		},
		{
			bytes.Repeat([]byte{0xff}, 16),
			new(big.Int).SetBytes(bytes.Repeat([]byte{0xff}, 16)),
		},
	}
	for i, test := range tests {
		id, err := NodeIDFromBytes(test.bytes)
		if err != nil {
			t.Errorf("test %v: unexpected error %v", i, err)
		}
		base10 := id.Base10()
		if base10.Cmp(test.base10) != 0 {
			t.Errorf("test %v: expected %v, got %v", i, test.base10, base10)
		}
	}
}

func TestNodeIDLess(t *testing.T) {
	tests := []struct {
		bytes1, bytes2 []byte
		less           bool
	}{
		{
			make([]byte, 16),
			make([]byte, 16),
			false,
		},
		{
			make([]byte, 16),
			bytes.Repeat([]byte{0x11}, 16),
			true,
		},
		{
			bytes.Repeat([]byte{0x11}, 16),
			make([]byte, 16),
			false,
		},
		{
			make([]byte, 16),
			bytes.Repeat([]byte{0xff}, 16),
			false,
		},
		{
			bytes.Repeat([]byte{0xff}, 16),
			make([]byte, 16),
			true,
		},
	}
	for i, test := range tests {
		id1, err := NodeIDFromBytes(test.bytes1)
		if err != nil {
			t.Errorf("test %v: unexpected error %v", i, err)
		}
		id2, err := NodeIDFromBytes(test.bytes2)
		if err != nil {
			t.Errorf("test %v: unexpected error %v", i, err)
		}
		less := id1.Less(id2)
		if less != test.less {
			t.Errorf("test %v: expected %v, got %v", i, test.less, less)
		}
	}
}

// Make sure that iterating over digits works correctly.
func TestNodeIDIterDigit(t *testing.T) {
	id, err := NodeIDFromBytes([]byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10})
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	for i := 0; i < 16; i++ {
		if digit := id.Digit(i); digit != byte(i) {
			t.Errorf("expected digit %#x, got %#x", i, digit)
		}
	}
	for i := 0; i < 16; i++ {
		if digit := id.Digit(16 + i); digit != byte(15-i) {
			t.Errorf("expected digit %#x, got %#x", 15-i, digit)
		}
	}
}

// Make sure an error is thrown if a NodeID is created from less than 32 bytes
func TestNodeIDFromBytesWithInsufficientBytes(t *testing.T) {
	bytes := []byte("123456789012345")
	id, err := NodeIDFromBytes(bytes)
	if err == nil {
		t.Errorf("Source length of %v bytes, but no error thrown. Instead returned NodeID of %v", len(bytes), id)
	}
}

// Make sure an error is *not* thrown if enough bytes are passed in.
func TestNodeIDFromBytesWithSufficientBytes(t *testing.T) {
	bytes := []byte("1234567890123456")
	_, err := NodeIDFromBytes(bytes)
	if err != nil {
		t.Errorf("Source length of %v bytes threw an error when no error should have been thrown.", len(bytes))
		t.Logf(err.Error())
	}
}

// Make sure the correct common prefix length is reported for two NodeIDs
func TestNodeIDCommonPrefixLen(t *testing.T) {
	n1 := NodeID{0xfdfdfdfdfdfdfdfd, 0xfdfdfdfdfdfdfdfd}
	n2 := NodeID{0xfdfdddfdfdfdfdfd, 0xfdfdfdfdfdfdfdfd}
	diff1 := 4

	n3 := NodeID{0xdfdfdfdfdfdfdfdf, 0xdfdfdfdfdfdfdfdf}
	n4 := NodeID{0xdfdfdfafdfdfdfdf, 0xdfdfdfdfdfdfdfdf}
	diff2 := 6

	if n1.CommonPrefixLen(n2) != diff1 {
		t.Errorf("Common prefix length should be %v, is %v instead.", diff1, n1.CommonPrefixLen(n2))
		t.Log(n1)
		t.Log(n2)
		if len(n1) > n1.CommonPrefixLen(n2) && len(n2) > n1.CommonPrefixLen(n2) {
			t.Logf("First significant digit: %v vs. %v", n1[n1.CommonPrefixLen(n2)], n2[n1.CommonPrefixLen(n2)])
		}
	}
	if n2.CommonPrefixLen(n3) != 0 {
		t.Errorf("Common prefix length should be %v, is %v instead.", 0, n2.CommonPrefixLen(n3))
		t.Log(n2)
		t.Log(n3)
		if len(n2) > n2.CommonPrefixLen(n3) && len(n3) > n2.CommonPrefixLen(n3) {
			t.Logf("First significant digit: %v vs. %v", n2[n2.CommonPrefixLen(n3)], n3[n2.CommonPrefixLen(n3)])
		}
	}
	if n3.CommonPrefixLen(n4) != diff2 {
		t.Errorf("Common prefix length should be %v, is %v instead.", diff2, n3.CommonPrefixLen(n4))
		t.Log(n3)
		t.Log(n4)
		if len(n3) > n3.CommonPrefixLen(n4) && len(n4) > n3.CommonPrefixLen(n4) {
			t.Logf("First significant digit: %v vs. %v", n3[n3.CommonPrefixLen(n4)], n4[n3.CommonPrefixLen(n4)])
		}
	}
	if n4.CommonPrefixLen(n4) != idLen {
		t.Errorf("Common prefix length should be %v, is %v instead.", len(n4), n4.CommonPrefixLen(n4))
		if n4.CommonPrefixLen(n4) < idLen {
			t.Logf("First significant digit: %v vs. %v", n4[n4.CommonPrefixLen(n4)], n4[n4.CommonPrefixLen(n4)])
		}
	}
}

// Make sure the correct difference is reported between NodeIDs
func TestNodeIDDiff(t *testing.T) {
	n1 := NodeID{0xfdfdfdfdfdfdfdfd, 0xfdfdfdfdfdfdfdfd}
	n2 := NodeID{0xfdfdfdfdfdfdfdfd, 0xfdfdfdfdfdfdfdfb}
	diff1 := n1.Diff(n2)
	if diff1.Cmp(big.NewInt(2)) != 0 {
		t.Errorf("Difference should be 2, was %v instead", diff1)
	}
	diff2 := n2.Diff(n1)
	if diff2.Cmp(big.NewInt(2)) != 0 {
		t.Errorf("Difference should be 2, was %v instead", diff2)
	}
	diff3 := n2.Diff(n2)
	if diff3.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("Difference should be 0, was %v instead", diff3)
	}
}

// Make sure NodeID comparisons wrap around the circle
func TestNodeIDDiffWrap(t *testing.T) {
	n1, err := NodeIDFromBytes(make([]byte, 16))
	if err != nil {
		t.Fatalf(err.Error())
	}
	n2, err := NodeIDFromBytes([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
	if err != nil {
		t.Fatalf(err.Error())
	}
	diff1 := n1.Diff(n2)
	if diff1.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("Difference should be 1, was %v instead", diff1)
	}
	diff2 := n2.Diff(n1)
	if diff2.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("Difference should be 1, was %v instead", diff2)
	}
	diff3 := n2.Diff(n2)
	if diff3.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("Difference should be 0, was %v instead", diff3)
	}
}

// Quick benchmark to test how expensive diffing nodes is
func BenchmarkNodeIDDiff(b *testing.B) {
	b.StopTimer()
	n1, err := NodeIDFromBytes(make([]byte, 16))
	if err != nil {
		b.Fatalf(err.Error())
	}
	n2, err := NodeIDFromBytes([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})
	if err != nil {
		b.Fatalf(err.Error())
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		n1.Diff(n2)
	}
}
