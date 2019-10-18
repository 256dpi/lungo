package bsonkit

import (
	"sort"

	"go.mongodb.org/mongo-driver/bson"
)

func Sort(list []bson.D, path string, reverse bool) ([]bson.D, error) {
	// prepare error
	var sortErr error

	// sort slice by comparing values
	sort.Slice(list, func(i, j int) bool {
		// get values
		a := Get(list[i], path)
		b := Get(list[j], path)

		// compare values
		res, err := Compare(a, b)
		if err != nil && sortErr == nil {
			sortErr = err
		}

		// check reverse
		if reverse {
			return res > 0
		}

		return res < 0
	})

	// check error
	if sortErr != nil {
		return nil, sortErr
	}

	return list, nil
}
