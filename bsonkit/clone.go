package bsonkit

import (
	"fmt"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func Clone(doc Doc) Doc {
	// clone document
	clone := cloneValue(*doc).(bson.D)

	return &clone
}

func CloneList(list List) List {
	// prepare clone
	clone := make(List, 0, len(list))

	// copy documents
	for _, doc := range list {
		clone = append(clone, Clone(doc))
	}

	return clone
}

func cloneValue(v interface{}) interface{} {
	switch value := v.(type) {
	case nil, int32, int64, float64, string, bool:
		// primitive do not need cloning
		return value
	case primitive.Null, primitive.ObjectID, primitive.Timestamp, time.Time, primitive.Regex:
		// basic structure do not need cloning
		return value
	case []byte, primitive.Binary:
		// do not clone binary data as they do not get mutated themselves
		return value
	case bson.D:
		// create new document
		d := make(bson.D, 0, len(value))

		// copy all elements and convert values
		for _, e := range value {
			d = append(d, bson.E{
				Key:   e.Key,
				Value: convertValue(e.Value),
			})
		}

		return d
	case bson.A:
		// create new array
		a := make(bson.A, 0, len(value))

		// copy all elements and convert them
		for _, e := range value {
			a = append(a, convertValue(e))
		}

		return a
	default:
		panic(fmt.Sprintf("clone: unsupported type: %q", reflect.TypeOf(v).String()))
	}
}
