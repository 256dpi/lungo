package mongokit

import (
	"fmt"
	"sort"

	"github.com/256dpi/lungo/bsonkit"
)

func Sort(list bsonkit.List, doc bsonkit.Doc) (bsonkit.List, error) {
	// prepare sort info
	paths := make([]string, 0, len(*doc))
	directions := make([]int, 0, len(*doc))

	// parse sort document
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
			return nil, fmt.Errorf("sort: expected number as direction")
		}

		// check direction
		if direction != -1 && direction != 1 {
			return nil, fmt.Errorf("sort: expected 1 or -1 as direction")
		}

		// add to info
		paths = append(paths, exp.Key)
		directions = append(directions, direction)
	}

	// sort slice by comparing values
	sort.Slice(list, func(i, j int) bool {
		for dir, path := range paths {
			// get values
			a := bsonkit.Get(list[i], path)
			b := bsonkit.Get(list[j], path)

			// compare values
			res := bsonkit.Compare(a, b)

			// continue if equal
			if res == 0 {
				continue
			}

			// check reverse
			if directions[dir] == -1 {
				return res > 0
			}

			return res < 0
		}

		return false
	})

	return list, nil
}
