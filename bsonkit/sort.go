package bsonkit

import (
	"sort"

	"go.mongodb.org/mongo-driver/bson"
)

func Sort(list []bson.D, path string, reverse bool) []bson.D {
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
