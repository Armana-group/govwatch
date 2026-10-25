// Package b58 implements Bitcoin-alphabet base58, the encoding Koinos uses
// for addresses and contract ids.
package b58

import (
	"errors"
	"math/big"
	"strings"
)

const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

var ErrInvalid = errors.New("b58: invalid character")

var radix = big.NewInt(58)

// Encode returns the base58 form of b. Leading zero bytes become leading '1's.
func Encode(b []byte) string {
	zeros := 0
	for zeros < len(b) && b[zeros] == 0 {
		zeros++
	}
	n := new(big.Int).SetBytes(b[zeros:])
	var out []byte
	mod := new(big.Int)
	for n.Sign() > 0 {
		n.DivMod(n, radix, mod)
		out = append(out, alphabet[mod.Int64()])
	}
	for i := 0; i < zeros; i++ {
		out = append(out, '1')
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

// Decode parses base58 text. Leading '1's become leading zero bytes.
func Decode(s string) ([]byte, error) {
	zeros := 0
	for zeros < len(s) && s[zeros] == '1' {
		zeros++
	}
	n := new(big.Int)
	for _, c := range []byte(s[zeros:]) {
		idx := strings.IndexByte(alphabet, c)
		if idx < 0 {
			return nil, ErrInvalid
		}
		n.Mul(n, radix)
		n.Add(n, big.NewInt(int64(idx)))
	}
	body := n.Bytes()
	out := make([]byte, zeros+len(body))
	copy(out[zeros:], body)
	return out, nil
}
