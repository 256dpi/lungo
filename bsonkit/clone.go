package bsonkit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Clone will clone the specified document. The returned document can be safely
// mutated without mutating the original document with one exception: the content
// of primitive.Binary values is not cloned and references the same byte slice
// as the original.
//
// The function may panic if the doc is not obtained using Convert or Transform
// and contains unsupported types.
func Clone(doc Doc) Doc {
	// check if nil
	if doc == nil {
		return nil
	}

	// clone document
	clone := cloneValue(*doc).(bson.D)

	return &clone
}

// CloneList will clone a list of documents.
//
// The function may panic if a doc is not obtained using Convert or Transform
// and contains unsupported types.
func CloneList(list List) List {
	// check if nil
	if list == nil {
		return nil
	}

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
		// primitives do not need cloning
		return value
	case primitive.Null, primitive.ObjectID, primitive.DateTime, primitive.Timestamp, primitive.Regex:
		// structures of primitives do not need cloning
		return value
	case primitive.Binary:
		// do not clone binary data as they do not get mutated themselves
		return value
	case bson.D:
		// create new document
		d := make(bson.D, 0, len(value))

		// copy all elements and convert values
		for _, e := range value {
			v, err := convertValue(e.Value)
			if err != nil {
				panic(err)
			}

			d = append(d, bson.E{
				Key:   e.Key,
				Value: v,
			})
		}

		return d
	case bson.A:
		// create new array
		a := make(bson.A, 0, len(value))

		// copy all elements and convert them
		for _, e := range value {
			v, err := convertValue(e)
			if err != nil {
				panic(err)
			}

			a = append(a, v)
		}

		return a
	default:
		panic(fmt.Sprintf("bsonkit: cannot clone: %T", v))
	}
}
