package bsonkit

import (
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Convert will convert a simple map to a document.
func Convert(m bson.M) Doc {
	d := convertMap(m)
	return &d
}

func convertMap(m bson.M) bson.D {
	// prepare m
	d := make(bson.D, 0, len(m))

	// copy keys
	for key, field := range m {
		d = append(d, bson.E{
			Key:   key,
			Value: convertValue(field),
		})
	}

	// sort document
	sort.Slice(d, func(i, j int) bool {
		return d[i].Key < d[j].Key
	})

	return d
}

func convertValue(v interface{}) interface{} {
	// convert recursively
	switch value := v.(type) {
	case bson.M:
		return convertMap(value)
	case map[string]interface{}:
		return convertMap(value)
	case bson.A:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = convertValue(item)
		}
		return a
	case []interface{}:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = convertValue(item)
		}
		return a
	case []string:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = item
		}
		return a
	case bson.D:
		d := make(bson.D, len(value))
		for i, item := range value {
			d[i].Key = item.Key
			d[i].Value = convertValue(item.Value)
		}
		return d
	case nil, int32, int64, float64, string, bool:
		return value
	case int:
		return int64(value)
	case primitive.Null, primitive.ObjectID, primitive.DateTime,
		primitive.Timestamp, primitive.Regex, primitive.Binary:
		return value
	default:
		panic(fmt.Sprintf("bsonkit: unsupported type %T", v))
	}
}
