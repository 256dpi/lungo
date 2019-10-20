package bsonkit

import "sort"

func Select(list List, limit int, fn func(Doc) (bool, bool)) List {
	// prepare result
	result := make(List, 0, len(list))

	// select documents
	for _, doc := range list {
		// match document
		matched, exit := fn(doc)
		if !matched && exit {
			break
		}

		// continue if document does not match
		if !matched {
			continue
		}

		// add to selection
		result = append(result, doc)

		// check exit
		if exit {
			break
		}

		// check limit
		if limit > 0 && len(result) >= limit {
			break
		}
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

func Collect(list List, path string, compact, distinct bool) []interface{} {
	// prepare result
	result := make([]interface{}, 0, len(list))

	// add values
	for _, doc := range list {
		// get value
		v := Get(doc, path)
		if v == Missing && compact {
			continue
		}

		// add value
		result = append(result, Get(doc, path))
	}

	// return early if not distinct
	if !distinct {
		return result
	}

	// sort results
	sort.Slice(result, func(i, j int) bool {
		return Compare(result[i], result[j]) < 0
	})

	// prepare distincts
	distincts := make([]interface{}, 0, len(result))

	// keep last value
	var lastValue interface{}

	// add distinct values
	for _, value := range result {
		// check if same as previous value
		if len(distincts) > 0 && Compare(lastValue, value) == 0 {
			continue
		}

		// add value
		distincts = append(distincts, value)
		lastValue = value
	}

	return distincts
}
