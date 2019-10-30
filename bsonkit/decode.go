package bsonkit

import (
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
)

// Decode will decode the specified document to an arbitrary value which may be
// a struct with bson tags in most cases.
func Decode(doc Doc, out interface{}) error {
	// the following approach is not very fast, but it ensures compatibility
	// with custom types that implement the bson.Unmarshaller interface

	// marshal document
	bytes, err := bson.Marshal(doc)
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

// DecodeList will decode a list of documents to an arbitrary value.
func DecodeList(list List, out interface{}) error {
	// get out value
	outValue := reflect.ValueOf(out)
	if outValue.Kind() != reflect.Ptr {
		return fmt.Errorf("results argument must be a pointer to a slice")
	}

	// get slice value and item type
	sliceVal := outValue.Elem()
	itemType := sliceVal.Type().Elem()

	for i, item := range list {
		// grow slice if at capacity
		if sliceVal.Len() == i {
			sliceVal = reflect.Append(sliceVal, reflect.New(itemType).Elem())
			sliceVal = sliceVal.Slice(0, sliceVal.Cap())
		}

		// get current element
		curItem := sliceVal.Index(i).Addr().Interface()

		// marshal item
		err := Decode(item, curItem)
		if err != nil {
			return err
		}
	}

	// re-slice and put result
	outValue.Elem().Set(sliceVal.Slice(0, len(list)))

	return nil
}
