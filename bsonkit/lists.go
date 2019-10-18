package bsonkit

import "sort"

func Difference(a, b List) List {
	// prepare result
	result := make(List, 0, len(a))

	// copy over items from a that are not in b
	var j int
	for _, item := range a {
		// skip if item is at head of b
		if j < len(b) && b[j] == item {
			j++
			continue
		}

		// otherwise add item to result
		result = append(result, item)
	}

	return result
}

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
