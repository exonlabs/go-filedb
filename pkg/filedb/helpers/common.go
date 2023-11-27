package helpers

import (
	"encoding/hex"
	"encoding/json"
	"slices"
)

// convert bytes to hex formated string "aa bb cc" with separator
func Hex(bytesArg []byte, sep string) string {
	h := hex.EncodeToString(bytesArg)
	if len(h) < 2 {
		return ""
	}
	// join string buffer with separator char
	buff := h[:2]
	for i := 2; i < len(h); i += 2 {
		buff += sep + h[i:i+2]
	}
	return buff
}

// Reverse the elements of the slice and return a copy
func RevCopy[S ~[]T, T any](s S) S {
	b := make([]T, len(s))
	copy(b, s)
	slices.Reverse(b)
	return b
}

// printable string for log messages
func Log(data any) string {
	b, _ := json.Marshal(data)
	return string(b)
}

// return True if all keys/values of match dict is found in src dict and
// are equal in values, else return False
func DictMatch(src, match map[string]any) bool {
	for key, val := range match {
		if v, ok := src[key]; !ok || v != val {
			return false
		}
	}
	return true
}

// return hex formated eui48 mac address "aa:bb:cc:dd:ee:ff"
func MacStr(bytesArg []byte) string {
	return Hex(bytesArg, ":")
}
