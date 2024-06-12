package bsonkit

import (
	"sort"

	"go.mongodb.org/mongo-driver/bson"
)

// Select will return a list of documents selected by the specified selector.
// Limit may be specified to break early if the list reached the limit.
func Select(list List, limit int, selector func(Doc) (bool, bool)) List {
	// prepare result
	var result List
	if limit > 0 {
		result = make(List, 0, limit)
	}

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

// Pick will get the value specified by path from each document and return a
// list of values. If compact is specified, Missing values are removed.
func Pick(list List, path string, compact bool) bson.A {
	// prepare result
	result := make(bson.A, 0, len(list))

	// add values
	for _, doc := range list {
		v := Get(doc, path)
		if compact && v == Missing {
			continue
		} else {
			result = append(result, v)
		}
	}

	return result
}

// Collect will get the value specified by path from each document and return a
// list of values. Different to Pick this function will also collect values from
// arrays of embedded documents. If compact is specified, Missing values are
// removed and intermediary arrays flattened. By enabling merge, a resulting array
// of embedded documents may be merged to on array containing all values. Flatten
// may flatten the resulting arrays per document to one array of values. Distinct
// may finally sort and remove duplicate values from the list.
func Collect(list List, path string, compact, merge, flatten, distinct bool) bson.A {
	// prepare result
	result := make(bson.A, 0, len(list))

	// add values
	for _, doc := range list {
		// get value
		v, _ := All(doc, path, compact, merge)
		if compact && v == Missing {
			continue
		}

		// flatten arrays if requested
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
		return Compare(result[i], result[j], nil) < 0
	})

	// prepare distincts
	distincts := make(bson.A, 0, len(result))

	// add distinct values
	var prevValue interface{}
	for _, value := range result {
		// check if same as previous value
		if len(distincts) > 0 && Compare(prevValue, value, nil) == 0 {
			continue
		}

		// add value
		distincts = append(distincts, value)
		prevValue = value
	}

	return distincts
}
