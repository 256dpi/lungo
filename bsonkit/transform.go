package bsonkit

import (
	"bytes"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
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
	return transfer(in, out, false)
}

func transfer(in, out interface{}, defaultDocumentM bool) error {
	// marshal to bytes
	var buf bytes.Buffer
	vw := bson.NewDocumentWriter(&buf)
	enc := bson.NewEncoder(vw)
	err := enc.Encode(in)
	if err != nil {
		return err
	}

	// unmarshal bytes
	vr := bson.NewDocumentReader(&buf)
	dec := bson.NewDecoder(vr)
	if defaultDocumentM {
		dec.DefaultDocumentM()
	}
	err = dec.Decode(out)
	if err != nil {
		return err
	}

	return nil
}
