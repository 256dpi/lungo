package bsonkit

import "go.mongodb.org/mongo-driver/v2/bson"

// ToMap converts a document to a bson.M for comparisons in tests.
func ToMap(doc Doc) bson.M {
	if doc == nil {
		return nil
	}
	out := make(bson.M, len(*doc))
	for _, elem := range *doc {
		out[elem.Key] = ToMapValue(elem.Value)
	}
	return out
}

// ToMapValue normalizes nested BSON documents to bson.M.
func ToMapValue(v interface{}) interface{} {
	switch x := v.(type) {
	case bson.D:
		m := make(bson.M, len(x))
		for _, e := range x {
			m[e.Key] = ToMapValue(e.Value)
		}
		return m
	case bson.M:
		m := make(bson.M, len(x))
		for k, val := range x {
			m[k] = ToMapValue(val)
		}
		return m
	case bson.A:
		a := make(bson.A, len(x))
		for i, val := range x {
			a[i] = ToMapValue(val)
		}
		return a
	default:
		return v
	}
}
