package bsonkit

import (
	"sort"
	"unsafe"

	"golang.org/x/text/collate"
)

// Column defines a column for ordering.
type Column struct {
	Path    string
	Reverse bool
}

// Sort will sort the list of documents in-place based on the specified columns.
func Sort(list List, columns []Column, identity bool, collator *collate.Collator) {
	// sort slice by comparing values
	sort.Slice(list, func(i, j int) bool {
		return Order(list[i], list[j], columns, identity, collator) < 0
	})
}

// Order will return the order of documents based on the specified columns.
func Order(l, r Doc, columns []Column, identity bool, collator *collate.Collator) int {
	for _, column := range columns {
		// get values
		a := Get(l, column.Path)
		b := Get(r, column.Path)

		// compare values
		res := Compare(a, b, collator)

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
