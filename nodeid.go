package pastry

import (
	"encoding/hex"
	"errors"
	"math/big"
)

// NodeIDDigit represents a single base 16 digit of a NodeID, stored as a byte (only half of which is used).
type NodeIDDigit byte

// NodeIDDigitsFromByte creates two NodeIDDigits from a single byte and returns them. The responses should add up to equal the byte that was passed in, as NodeIDDigitsFromByte simply cuts the byte in half using bit-shifting and returns the halves.
func NodeIDDigitsFromByte(b byte) (NodeIDDigit, NodeIDDigit) {
	// 0xf = 00001111, 0xf0 = 11110000
	// b&0xf0 = first four bits, b&0xf = last four bits
	return NodeIDDigit(b & 0xf0), NodeIDDigit(b & 0xf)
}

// Canonical returns the NodeIDDigit such that it can be safely compared to other NodeIDDigits by standardising which half of the byte is insignificant.
func (d NodeIDDigit) Canonical() NodeIDDigit {
	if d > 0xf {
		return d >> 4
	}
	return d
}

// String returns the NodeIDDigit encoded as a hexadecimal string with the insignificant half of the byte stripped from the string.
func (d NodeIDDigit) String() string {
	asHex := hex.EncodeToString([]byte{byte(d.Canonical())})
	return string(asHex[1])
}

// Equals tests two NodeIDDigits for equality, returning true if the digits are considered to be equal and false if they are considered to be inequal. NodeIDDigits are considered to be equal if the significant halves of the bytes that represent them are equal.
func (d NodeIDDigit) Equals(other NodeIDDigit) bool {
	return d.Canonical() == other.Canonical()
}

// Less tests two NodeIDDigits to determine whether the argument is less than the digit the method is being called on. A digit is considered to be less if its significant half of a byte is less than the significant half of the other digit's byte.
func (d NodeIDDigit) Less(other NodeIDDigit) bool {
	return d.Canonical() < other.Canonical()
}

// NodeID is a unique address for a node in the network. It is an array of 32 NodeIDDigits.
type NodeID []NodeIDDigit

// NodeIDFromBytes creates a NodeID from an array of bytes.
// It returns the created NodeID, trimmed to the first 32 digits, or nil and an error if there are not enough bytes to yield 32 digits.
func NodeIDFromBytes(source []byte) (NodeID, error) {
	var result NodeID
	if len(source) < 16 {
		return nil, errors.New("Not enough bytes to create a NodeID.")
	}
	for _, b := range source {
		d1, d2 := NodeIDDigitsFromByte(b)
		result = append(result, d1, d2)
	}
	result = result[:32]
	return result, nil
}

// String returns the hexadecimal string encoding of each NodeIDDigit in the NodeID, discarding the insignificant half of the byte.
func (id NodeID) String() string {
	result := ""
	for _, digit := range id {
		result += digit.String()
	}
	return result
}

// Equals tests two NodeIDs for equality and returns true if they are considered equal, false if they are considered inequal. NodeIDs are considered equal if each digit of the NodeID is equal.
func (id NodeID) Equals(other NodeID) bool {
	for i, d := range id {
		if !d.Equals(other[i]) {
			return false
		}
	}
	return true
}

// Less tests two NodeIDs to determine if the ID the method is called on is less than the ID passed as an argument. An ID is considered to be less if the first inequal digit between the two IDs is considered to be less.
func (id NodeID) Less(other NodeID) bool {
	for i, d := range id {
		if !d.Equals(other[i]) {
			return d.Less(other[i])
		}
	}
	return false
}

// CommonPrefixLen returns the number of leading digits that are equal in the two NodeIDs.
func (id NodeID) CommonPrefixLen(other NodeID) int {
	for i, d := range id {
		if !d.Equals(other[i]) {
			return i
		}
	}
	return len(id)
}

// Diff returns the difference between two NodeIDs as an absolute value. It performs the modular arithmetic necessary to find the shortest distance between the IDs in the (2^128)-1 item nodespace.
func (id NodeID) Diff(other NodeID) *big.Int {
	max := big.NewInt(0).Exp(big.NewInt(2), big.NewInt(128), nil)
	id10 := id.Base10()
	other10 := other.Base10()
	middle := big.NewInt(0).Div(max, big.NewInt(2))
	larger := big.NewInt(0)
	smaller := big.NewInt(0)
	if id10.Cmp(other10) > 0 {
		larger = id10
		smaller = other10
	} else {
		larger = other10
		smaller = id10
	}
	diff := big.NewInt(0).Sub(larger, smaller)
	if diff.Cmp(middle) <= 0 {
		return diff
	}
	res := big.NewInt(0).Sub(max, larger)
	res = res.Add(res, smaller)
	res = res.Mod(res, max)
	return res
}

// RelPos uses modular arithmetic to determine whether the NodeID passed as an argument is to the left of the NodeID it is called on (-1), the same as the NodeID it is called on (0), or to the right of the NodeID it is called on (1) in thee circular node space.
func (id NodeID) RelPos(other NodeID) int {
	max := big.NewInt(0).Exp(big.NewInt(2), big.NewInt(128), nil)
	id10 := id.Base10()
	other10 := other.Base10()
	middle := big.NewInt(0).Div(max, big.NewInt(2))
	larger := big.NewInt(0)
	smaller := big.NewInt(0)
	if id10.Cmp(other10) > 0 {
		larger = id10
		smaller = other10
	} else {
		larger = other10
		smaller = id10
	}
	diff := big.NewInt(0).Sub(larger, smaller)
	if diff.Cmp(middle) < 0 {
		return 1
	} else if diff.Cmp(middle) == 0 {
		return 0
	}
	return 1
}

// Base10 returns the NodeID as a base 10 number, translating each base 16 digit.
func (id NodeID) Base10() *big.Int {
	res := big.NewInt(0)
	for i, d := range id {
		res = res.Add(res, big.NewInt(0).Mul(big.NewInt(int64(d.Canonical())), big.NewInt(0).Exp(big.NewInt(16), big.NewInt(int64(len(id)-1-i)), nil)))
	}
	return res
}

// MarshalJSON fulfills the Marshaler interface, allowing NodeIDs to be serialised to JSON safely.
func (id *NodeID) MarshalJSON() ([]byte, error) {
	return []byte(id.String()), nil
}

// UnmarshalJSON fulfills the Unmarshaler interface, allowing NodeIDs to be unserialised from JSON safely.
func (id *NodeID) UnmarshalJSON(source []byte) error {
	if id == nil {
		return errors.New("UnmarshalJSON on nil NodeID.")
	}
	new_id, err := NodeIDFromBytes(source)
	if err != nil {
		return err
	}
	*id = append((*id)[0:0], new_id...)
	return nil
}
