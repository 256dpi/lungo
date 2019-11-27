package mongokit

import (
	"fmt"

	"github.com/256dpi/lungo/bsonkit"
)

// Columns will return columns from a MongoDB sort document.
func Columns(doc bsonkit.Doc) ([]bsonkit.Column, error) {
	// prepare columns
	columns := make([]bsonkit.Column, 0, len(*doc))
	for _, exp := range *doc {
		// get direction
		var direction int
		switch value := exp.Value.(type) {
		case int32:
			direction = int(value)
		case int64:
			direction = int(value)
		case float64:
			direction = int(value)
		default:
			return nil, fmt.Errorf("expected number as direction")
		}

		// check direction
		if direction != -1 && direction != 1 {
			return nil, fmt.Errorf("expected 1 or -1 as direction")
		}

		// add column
		columns = append(columns, bsonkit.Column{
			Path:    exp.Key,
			Reverse: direction == -1,
		})
	}

	return columns, nil
}

// Sort will sort a list based on a MongoDB sort document and return a new
// list with sorted documents.
func Sort(list bsonkit.List, doc bsonkit.Doc) (bsonkit.List, error) {
	// copy list
	result := make(bsonkit.List, len(list))
	copy(result, list)

	// prepare columns
	columns, err := Columns(doc)
	if err != nil {
		return nil, err
	}

	// sort list
	bsonkit.Sort(result, columns, true)

	return result, nil
}
