package bsonkit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// Transform will transform an arbitrary value into a document composed of known
// primitives.
func Transform(v interface{}) (Doc, error) {
	// transfer
	var doc bson.D
	err := Transfer(v, &doc)
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

// Transfer will transfer data from one type to another by marshalling the data
// and unmarshalling it again. This method is not very fast, but it ensures
// compatibility with custom types that implement the bson.Marshaller interface.
func Transfer(in, out interface{}) error {
	// marshal to bytes
	bytes, err := bson.Marshal(in)
	if err != nil {
		return err
	}

	// unmarshal bytes
	err = bson.Unmarshal(bytes, out)
	if err != nil {
		return err
	}

	return nil
}
