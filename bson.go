package lungo

import (
	"fmt"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
)

// TODO: This approach is slow, we should do it directly in memory if possible.

func ReduceDocument(doc interface{}) (bson.M, error) {
	// marshal to bytes
	bytes, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}

	// unmarshal bytes
	var m bson.M
	err = bson.Unmarshal(bytes, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func DecodeItem(item bson.M, out interface{}) error {
	// marshal item
	bytes, err := bson.Marshal(item)
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

func DecodeList(list []bson.M, out interface{}) error {
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
		err := DecodeItem(item, curItem)
		if err != nil {
			return err
		}
	}

	// re-slice and set result
	outValue.Elem().Set(sliceVal.Slice(0, len(list)))

	return nil
}
