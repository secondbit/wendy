package wendy

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"

	peer "github.com/libp2p/go-libp2p-core/peer"
)

const idLen = 32

// NodeID is a unique address for a node in the network.
type NodeID [2]uint64

// NodeIDFromBytes creates a NodeID from an array of bytes.
// It returns the created NodeID, trimmed to the first 32 digits, or nil and an error if there are not enough bytes to yield 32 digits.
func NodeIDFromBytes(source []byte) (NodeID, error) {
	var result NodeID
	if len(source) < 16 {
		return [2]uint64{}, errors.New("not enough bytes to create a nodeid")
	}
	result[0] = binary.BigEndian.Uint64(source)
	result[1] = binary.BigEndian.Uint64(source[8:])
	return result, nil
}

// NodeIDFromPeerID creates a NodeID from a libp2p peer ID
func NodeIDFromPeerID(pid peer.ID) (NodeID, error) {
	var result NodeID
	if len(pid) < 16 {
		return [2]uint64{}, errors.New("not enough bytes to create a nodeid")
	}
	pidBytes, err := pid.Marshal()
	if err != nil {
		return [2]uint64{}, err
	}
	result[0] = binary.BigEndian.Uint64(pidBytes)
	result[1] = binary.BigEndian.Uint64(pidBytes[8:])
	return result, nil
}

// String returns the hexadecimal string encoding of the NodeID.
func (id NodeID) String() string {
	return fmt.Sprintf("%016x%016x", id[0], id[1])
}

// Equals tests two NodeIDs for equality and returns true if they are considered equal, false if they are considered inequal. NodeIDs are considered equal if each digit of the NodeID is equal.
func (id NodeID) Equals(other NodeID) bool {
	return id[0] == other[0] && id[1] == other[1]
}

// Less tests two NodeIDs to determine if the ID the method is called on is less than the ID passed as an argument. An ID is considered to be less if the first inequal digit between the two IDs is considered to be less.
func (id NodeID) Less(other NodeID) bool {
	return id.RelPos(other) < 0
}

// absLess returns true if id < other, disregarding modular arithmetic.
func (id NodeID) absLess(other NodeID) bool {
	return id[0] < other[0] || id[0] == other[0] && id[1] < other[1]
}

// TODO(eds): this could be faster and smaller with a little assembly, but not
// sure if we want to go there.

// digitSet returns the index of the first 4-bit digit with any bits set.
// The most significant digit is digit 0; the least significant is digit 15.
func digitSet(x uint64) int {
	if x&0xffffffff00000000 != 0 {
		if x&0xffff000000000000 != 0 {
			if x&0xff00000000000000 != 0 {
				if x&0xf000000000000000 != 0 {
					return 0
				}
				return 1
			}
			if x&0x00f0000000000000 != 0 {
				return 2
			}
			return 3
		}
		if x&0x0000ff0000000000 != 0 {
			if x&0x0000f00000000000 != 0 {
				return 4
			}
			return 5
		}
		if x&0x000000f000000000 != 0 {
			return 6
		}
		return 7
	}
	if x&0x00000000ffff0000 != 0 {
		if x&0x00000000ff000000 != 0 {
			if x&0x00000000f0000000 != 0 {
				return 8
			}
			return 9
		}
		if x&0x00000000f0000000 != 0 {
			return 10
		}
		return 11
	}
	if x&0x000000000000ff00 != 0 {
		if x&0x000000000000f000 != 0 {
			return 12
		}
		return 13
	}
	if x&0x00000000000000f0 != 0 {
		return 14
	}
	return 15
}

// CommonPrefixLen returns the number of leading digits that are equal in the two NodeIDs.
func (id NodeID) CommonPrefixLen(other NodeID) int {
	if xor := id[0] ^ other[0]; xor != 0 {
		return digitSet(xor)
	}
	if xor := id[1] ^ other[1]; xor != 0 {
		return digitSet(xor) | 16
	}
	return idLen
}

// differences returns the difference between the two NodeIDs in both directions.
func (id NodeID) differences(other NodeID) (NodeID, NodeID) {
	var d1, d2 NodeID
	if id.absLess(other) {
		d1[1] = other[1] - id[1]
		// check for borrow
		b := 0
		if d1[1] > other[1] {
			b = 1
		}
		d1[0] = other[0] - (id[0] + uint64(b))
		d2[0], d2[1] = math.MaxUint64-d1[0], math.MaxUint64-d1[1]+1
	} else {
		d2[1] = id[1] - other[1]
		// check for borrow
		b := 0
		if d2[1] > id[1] {
			b = 1
		}
		d2[0] = id[0] - (other[0] + uint64(b))
		d1[0], d1[1] = math.MaxUint64-d2[0], math.MaxUint64-d2[1]+1
	}
	return d2, d1
}

// Diff returns the difference between two NodeIDs as an absolute value. It performs the modular arithmetic necessary to find the shortest distance between the IDs in the (2^128)-1 item nodespace.
func (id NodeID) Diff(other NodeID) *big.Int {
	d1, d2 := id.differences(other)
	if d1.absLess(d2) {
		return d1.Base10()
	}
	return d2.Base10()
}

// RelPos uses modular arithmetic to determine whether the NodeID passed as an argument is to the left of the NodeID it is called on (-1), the same as the NodeID it is called on (0), or to the right of the NodeID it is called on (1) in the circular node space.
func (id NodeID) RelPos(other NodeID) int {
	if id.Equals(other) {
		return 0
	}
	d1, d2 := id.differences(other)
	if d1.absLess(d2) {
		return 1
	}
	return -1
}

var one = big.NewInt(1)

// Base10 returns the NodeID as a base 10 number, translating each base 16 digit.
func (id NodeID) Base10() *big.Int {
	var result big.Int
	if id[0] > math.MaxInt64 {
		result.SetInt64(math.MaxInt64)
		result.Add(&result, one)
		result.Lsh(&result, 64)
		id[0] -= math.MaxInt64 + 1
	}
	var tmp big.Int
	tmp.SetInt64(int64(id[0]))
	tmp.Lsh(&tmp, 64)
	result.Add(&result, &tmp)
	if id[1] > math.MaxInt64 {
		tmp.SetInt64(math.MaxInt64)
		result.Add(&result, &tmp)
		result.Add(&result, one)
		id[1] -= math.MaxInt64 + 1
	}
	tmp.SetInt64(int64(id[1]))
	result.Add(&result, &tmp)
	return &result
}

// MarshalJSON fulfills the Marshaler interface, allowing NodeIDs to be serialised to JSON safely.
func (id NodeID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + id.String() + `"`), nil
}

// UnmarshalJSON fulfills the Unmarshaler interface, allowing NodeIDs to be unserialised from JSON safely.
func (id *NodeID) UnmarshalJSON(source []byte) error {
	if id == nil {
		return errors.New("UnmarshalJSON on nil nodeid")
	}
	var str string
	err := json.Unmarshal(source, &str)
	if err != nil {
		return err
	}
	dec, err := hex.DecodeString(str)
	if err != nil {
		return err
	}
	newID, err := NodeIDFromBytes([]byte(dec))
	if err != nil {
		return err
	}
	*id = newID
	return nil
}

// Digit returns the ith 4-bit digit in the NodeID. If i >= 32, Digit panics.
func (id NodeID) Digit(i int) byte {
	if uint(i) >= 32 {
		panic("invalid digit index")
	}
	n := id[0]
	if i >= 16 {
		n = id[1]
		i &= 15
	}
	k := 4 * uint(15-i)
	return byte((n >> k) & 0xf)
}
