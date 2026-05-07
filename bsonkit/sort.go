package bsonkit

import (
	"sort"
	"unsafe"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Column defines a column for ordering.
type Column struct {
	Path    string
	Reverse bool
}

// Sort will sort the list of documents in-place based on the specified columns.
// Documents with equal column values retain their original (insertion) order,
// matching MongoDB's stable-sort semantics.
func Sort(list List, columns []Column) {
	sort.SliceStable(list, func(i, j int) bool {
		return Order(list[i], list[j], columns, false) < 0
	})
}

// sortKey reduces an array value to the element MongoDB uses for ordering:
// the smallest for ascending sorts and the largest for descending sorts. Any
// other value (including Missing and empty arrays) is returned as-is.
//
// MongoDB picks the per-document element rather than comparing whole arrays
// element-by-element, so e.g. ascending sort of `[3, 1]` and `[2]` yields
// `[3, 1]` first (min=1 < min=2), not `[2]` first (which lexicographic
// compare would produce).
func sortKey(v interface{}, reverse bool) interface{} {
	arr, ok := v.(bson.A)
	if !ok || len(arr) == 0 {
		return v
	}
	best := arr[0]
	for _, item := range arr[1:] {
		cmp := Compare(item, best)
		if reverse {
			if cmp > 0 {
				best = item
			}
		} else {
			if cmp < 0 {
				best = item
			}
		}
	}
	return best
}

// Order will return the order of documents based on the specified columns.
func Order(l, r Doc, columns []Column, identity bool) int {
	for _, column := range columns {
		// get values, reducing arrays to the per-direction sort key
		a := sortKey(Get(l, column.Path), column.Reverse)
		b := sortKey(Get(r, column.Path), column.Reverse)

		// compare values
		res := Compare(a, b)

		// continue if equal
		if res == 0 {
			continue
		}

		// check if reverse
		if column.Reverse {
			return res * -1
		}

		return res
	}

	// return if identity should not be checked
	if !identity {
		return 0
	}

	// get addresses
	al := uintptr(unsafe.Pointer(l))
	ar := uintptr(unsafe.Pointer(r))

	// compare identity
	if al == ar {
		return 0
	} else if al < ar {
		return -1
	} else {
		return 1
	}
}
