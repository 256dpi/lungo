package bsonkit

import (
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Convert will convert the provided value to a document. The value is expected
// to be a bson.M or bson.D composed of standard types.
func Convert(v interface{}) Doc {
	// convert value
	doc, ok := convertValue(v).(bson.D)
	if !ok {
		panic(`bsonkit: expected conversion to result in a "bson.D"`)
	}

	return &doc
}

// ConvertList will convert an array to a list. The value is expected to be a
// bson.A of bson.M or bson.D elements composed of standard types.
func ConvertList(v interface{}) List {
	// convert value
	doc := convertValue(v)

	// get array
	array, ok := doc.(bson.A)
	if !ok {
		panic(`bsonkit: expected array`)
	}

	// build list
	list := make(List, 0, len(array))
	for _, item := range array {
		doc, ok := item.(bson.D)
		if !ok {
			panic(`bsonkit: expected array of documents`)
		}
		list = append(list, &doc)
	}

	return list
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
	case []primitive.ObjectID:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = item
		}
		return a
	case nil, int32, int64, float64, string, bool:
		return value
	case int:
		return int64(value)
	case primitive.Null, primitive.ObjectID, primitive.DateTime,
		primitive.Timestamp, primitive.Regex, primitive.Binary:
		return value
	case *primitive.ObjectID:
		if value != nil {
			return *value
		} else {
			return nil
		}
	case time.Time:
		return primitive.NewDateTimeFromTime(value.UTC())
	case *time.Time:
		if value != nil {
			return primitive.NewDateTimeFromTime(value.UTC())
		} else {
			return nil
		}
	default:
		panic(fmt.Sprintf("bsonkit: unsupported type %T", v))
	}
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
