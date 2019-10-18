package mongokit

import (
	"github.com/256dpi/lungo/bsonkit"
)

func Filter(list bsonkit.List, query bsonkit.Doc, limit int) (bsonkit.List, error) {
	// filter list based on query
	var result bsonkit.List
	for _, item := range list {
		// match item
		res, err := Match(item, query)
		if err != nil {
			return nil, err
		} else if res {
			result = append(result, item)
		}

		// check limit
		if limit > 0 && len(result) >= limit {
			return result, nil
		}
	}

	return result, nil
}
