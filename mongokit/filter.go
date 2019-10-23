package mongokit

import (
	"github.com/256dpi/lungo/bsonkit"
)

func Filter(list bsonkit.List, query bsonkit.Doc, limit int) (bsonkit.List, error) {
	// select documents
	var matchErr error
	result := bsonkit.Select(list, limit, func(doc bsonkit.Doc) (bool, bool) {
		// match based on query
		res, err := Match(doc, query)
		if err != nil {
			matchErr = err
			return false, true
		}

		return res, false
	})
	if matchErr != nil {
		return result, matchErr
	}

	return result, nil
}
