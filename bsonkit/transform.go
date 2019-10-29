package bsonkit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// Transform will transform an arbitrary value into a document composed of known
// primitives.
func Transform(v interface{}) (Doc, error) {
	// TODO: This approach is slow, we should do it directly in memory if possible.

	// marshal to bytes
	bytes, err := bson.Marshal(v)
	if err != nil {
		return nil, err
	}

	// unmarshal bytes
	var doc bson.D
	err = bson.Unmarshal(bytes, &doc)
	if err != nil {
		return nil, err
	}

	return &doc, nil
}

// TransformList will transform an arbitrary value info a list of documents
// composed of known primitives.
func TransformList(v interface{}) (List, error) {
	// transform value
	doc, err := Transform(bson.M{"v": v})
	if err != nil {
		return nil, err
	}

	// get array
	array, ok := (*doc)[0].Value.(bson.A)
	if !ok {
		return nil, fmt.Errorf("expected array")
	}

	// build list
	list := make(List, 0, len(array))
	for _, item := range array {
		doc, ok := item.(bson.D)
		if !ok {
			return nil, fmt.Errorf("expected array of documents")
		}
		list = append(list, &doc)
	}

	return list, nil
}
