package bsonkit

import (
	"sort"

	"go.mongodb.org/mongo-driver/bson"
)

func Convert(m bson.M) bson.D {
	// prepare m
	d := make(bson.D, 0, len(m))

	// copy keys
	for key, field := range m {
		d = append(d, bson.E{
			Key:   key,
			Value: convert(field),
		})
	}

	// sort document
	sort.Slice(d, func(i, j int) bool {
		return d[i].Key < d[j].Key
	})

	return d
}

func convert(v interface{}) interface{} {
	// convert recursively
	switch value := v.(type) {
	case bson.M:
		return Convert(value)
	case bson.A:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = convert(item)
		}
		return a
	case bson.D:
		d := make(bson.D, len(value))
		for i, item := range value {
			d[i].Key = item.Key
			d[i].Value = convert(item.Value)
		}
		return d
	default:
		return v
	}
}
