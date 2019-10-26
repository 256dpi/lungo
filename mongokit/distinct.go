package mongokit

import (
	"sort"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func Distinct(list bsonkit.List, path string) bson.A {
	// prepare result
	result := make(bson.A, 0, len(list))

	// add values
	for _, doc := range list {
		value,_ := bsonkit.All(doc, path, true, true)
		if array, ok := value.(bson.A); ok {
			result = append(result, array...)
		} else if value != bsonkit.Missing {
			result = append(result, value)
		}
	}

	// sort results
	sort.Slice(result, func(i, j int) bool {
		return bsonkit.Compare(result[i], result[j]) < 0
	})

	// prepare distincts
	distincts := make(bson.A, 0, len(result))

	// keep last value
	var lastValue interface{}

	// add distinct values
	for _, value := range result {
		// check if same as previous value
		if len(distincts) > 0 && bsonkit.Compare(lastValue, value) == 0 {
			continue
		}

		// add value
		distincts = append(distincts, value)
		lastValue = value
	}

	return distincts
}
