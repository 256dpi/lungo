package mongokit

import (
	"github.com/256dpi/lungo/bsonkit"
)

func Filter(list bsonkit.List, query bsonkit.Doc, limit int) (bsonkit.List, error) {
	// prepare match error
	var matchErr error

	// select documents
	result := bsonkit.Select(list, limit, func(doc bsonkit.Doc) (bool, bool) {
		// match based on query
		res, err := Match(doc, query)
		if err != nil {
			matchErr = err
			return false, true
		}

		return res, false
	})

	// check error
	if matchErr != nil {
		return result, matchErr
	}

	return result, nil
}
