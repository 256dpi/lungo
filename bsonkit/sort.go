package bsonkit

import (
	"sort"
)

func Sort(list List, path string, reverse bool) List {
	// sort slice by comparing values
	sort.Slice(list, func(i, j int) bool {
		// get values
		a := Get(list[i], path)
		b := Get(list[j], path)

		// compare values
		res := Compare(a, b)

		// check reverse
		if reverse {
			return res > 0
		}

		return res < 0
	})

	return list
}
