package bsonkit

import (
	"sort"

	"go.mongodb.org/mongo-driver/bson"
)

func Select(list List, limit int, selector func(Doc) (bool, bool)) List {
	// prepare result
	result := make(List, 0, len(list))

	// select documents
	for _, doc := range list {
		// run selector
		selected, exit := selector(doc)
		if !selected && exit {
			break
		}

		// continue if document has not been selected
		if !selected {
			continue
		}

		// add to result
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

func Collect(list List, path string, compact, flatten, distinct bool) bson.A {
	// prepare result
	result := make(bson.A, 0, len(list))

	// add values
	for _, doc := range list {
		// get value
		v, _ := All(doc, path, compact, true)
		if v == Missing && compact {
			continue
		}

		// add values or value
		if a, ok := v.(bson.A); ok && flatten {
			result = append(result, a...)
		} else {
			result = append(result, v)
		}
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
	distincts := make(bson.A, 0, len(result))

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
