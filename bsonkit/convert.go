package bsonkit

import (
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Convert will convert a simple map to a document.
func Convert(m bson.M) Doc {
	d := convertMap(m)
	return &d
}

// ConvertList will convert a simple array to a list.
func ConvertList(a []bson.M) List {
	// convert all elements
	l := make(List, len(a))
	for i, item := range a {
		l[i] = Convert(item)
	}

	return l
}

func convertMap(m bson.M) bson.D {
	// prepare document
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
	case []bson.M:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = convertValue(item)
		}
		return a
	case bson.D:
		d := make(bson.D, len(value))
		for i, item := range value {
			d[i].Key = item.Key
			d[i].Value = convertValue(item.Value)
		}
		return d
	case []bson.D:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = convertValue(item)
		}
		return a
	case nil, int32, int64, float64, string, bool:
		return value
	case int:
		return int64(value)
	case primitive.Null, primitive.ObjectID, primitive.DateTime,
		primitive.Timestamp, primitive.Regex, primitive.Binary:
		return value
	case time.Time:
		return primitive.NewDateTimeFromTime(value.UTC())
	default:
		panic(fmt.Sprintf("bsonkit: unsupported type %T", v))
	}
}
