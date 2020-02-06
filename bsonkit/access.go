package bsonkit

import (
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
)

// MissingType is the type of the Missing value.
type MissingType struct{}

// Missing is returned by Get if the value cannot be found in the document.
var Missing = MissingType{}

var unsetValue interface{} = struct{}{}

// Get returns the value in the document specified by path. It returns Missing
// if the value has not been found. Dots may be used to descend into nested
// documents e.g. "foo.bar.baz" and numbers may be used to descend into arrays
// e.g. "foo.2.bar".
func Get(doc Doc, path string) interface{} {
	value, _ := get(doc, path, false, false)
	return value
}

// All has the basic behaviour as Get but additionally collects values from
// embedded documents in arrays. It returns and array and true if values from
// multiple documents haven been collected. Missing values are skipped and
// intermediary arrays flattened if compact is set to true. By enabling merge,
// a resulting array of embedded document may be merged to on array containing
// all values.
func All(doc Doc, path string, compact, merge bool) (interface{}, bool) {
	// get value
	value, nested := get(doc, path, true, compact)
	if !nested || !merge {
		return value, nested
	}

	// get array
	array, ok := value.(bson.A)
	if !ok {
		return value, nested
	}

	// prepare result
	result := make(bson.A, 0, len(array))

	// merge arrays
	for _, item := range array {
		if a, ok := item.(bson.A); ok {
			for _, i := range a {
				result = append(result, i)
			}
		} else {
			result = append(result, item)
		}
	}

	return result, nested
}

func get(v interface{}, path string, collect, compact bool) (interface{}, bool) {
	// check path
	if path == PathEnd {
		return v, false
	}

	// check if empty
	if path == "" {
		return Missing, false
	}

	// get key
	key := PathSegment(path)

	// get document field
	if doc, ok := v.(bson.D); ok {
		for _, el := range doc {
			if el.Key == key {
				return get(el.Value, PathReduce(path), collect, compact)
			}
		}
	}

	// get document field (with pointer)
	if doc, ok := v.(*bson.D); ok {
		for _, el := range *doc {
			if el.Key == key {
				return get(el.Value, PathReduce(path), collect, compact)
			}
		}
	}

	// get array field
	if arr, ok := v.(bson.A); ok {
		// get indexed array element if number
		if index, ok := ParseIndex(key); ok {
			if index >= 0 && index < len(arr) {
				return get(arr[index], PathReduce(path), collect, compact)
			}
		}

		// collect values from embedded documents
		if collect {
			res := make(bson.A, 0, len(arr))
			for _, item := range arr {
				value, ok := get(item, path, collect, compact)
				if value == Missing && !compact {
					res = append(res, value)
				} else if value != Missing {
					if ok && compact {
						res = append(res, value.(bson.A)...)
					} else {
						res = append(res, value)
					}
				}
			}
			return res, true
		}
	}

	return Missing, false
}

// Put will store the value in the document at the location specified by path
// and return the previously stored value. It will automatically create document
// fields, array elements and embedded documents to fulfill the request. If
// prepends is set to true, new values are inserted at the beginning of the array
// or document. If the path contains a number e.g. "foo.1.bar" and no array
// exists at that levels, a document with the key "1" is created.
func Put(doc Doc, path string, value interface{}, prepend bool) (interface{}, error) {
	res, ok := put(*doc, path, value, prepend, func(v interface{}) {
		*doc = v.(bson.D)
	})
	if !ok {
		return nil, fmt.Errorf("cannot put value at %s", path)
	}

	return res, nil
}

// Unset will remove the value at the location in the document specified by path
// and return the previously stored value. If the path specifies an array element
// e.g. "foo.2" the element is nilled, but not removed from the array. This
// prevents unintentional effects through position shifts in the array.
func Unset(doc Doc, path string) interface{} {
	res, _ := put(*doc, path, unsetValue, false, func(v interface{}) {
		*doc = v.(bson.D)
	})

	return res
}

func put(v interface{}, path string, value interface{}, prepend bool, set func(interface{})) (interface{}, bool) {
	// check path
	if path == PathEnd {
		set(value)
		return v, true
	}

	// check if empty
	if path == "" {
		return Missing, false
	}

	// get key
	key := PathSegment(path)

	// put document field
	if doc, ok := v.(bson.D); ok {
		for i, el := range doc {
			if el.Key == key {
				return put(doc[i].Value, PathReduce(path), value, prepend, func(v interface{}) {
					if v == unsetValue {
						set(append(doc[:i], doc[i+1:]...))
					} else {
						doc[i].Value = v
					}
				})
			}
		}

		// check if unset
		if value == unsetValue {
			return Missing, false
		}

		// capture value
		e := bson.E{Key: key}
		res, ok := put(Missing, PathReduce(path), value, prepend, func(v interface{}) {
			e.Value = v
		})
		if !ok {
			return res, false
		}

		// set appended/prepended document
		if prepend {
			set(append(bson.D{e}, doc...))
		} else {
			set(append(doc, e))
		}

		return Missing, true
	}

	// put array field
	if arr, ok := v.(bson.A); ok {
		index, err := strconv.Atoi(key)
		if err != nil || index < 0 {
			return Missing, false
		}

		// update existing element
		if index < len(arr) {
			return put(arr[index], PathReduce(path), value, prepend, func(v interface{}) {
				if v == unsetValue {
					arr[index] = nil
				} else {
					arr[index] = v
				}
			})
		}

		// check if unset
		if value == unsetValue {
			return Missing, false
		}

		// fill with nil elements
		for i := len(arr); i < index+1; i++ {
			arr = append(arr, nil)
		}

		// put in last element
		res, ok := put(Missing, PathReduce(path), value, prepend, func(v interface{}) {
			arr[index] = v
		})
		if !ok {
			return res, false
		}

		// set array
		set(arr)

		return Missing, true
	}

	// check if unset
	if value == unsetValue {
		return Missing, false
	}

	// put new document
	if v == Missing {
		// capture value
		e := bson.E{Key: key}
		res, ok := put(Missing, PathReduce(path), value, prepend, func(v interface{}) {
			e.Value = v
		})
		if !ok {
			return res, false
		}

		// set document
		set(bson.D{e})

		return Missing, true
	}

	return Missing, false
}

// Increment will add the increment to the value at the location in the document
// specified by path and return the new value. If the value is missing, the
// increment is added to the document. The type of the field may be changed as
// part of the operation.
func Increment(doc Doc, path string, increment interface{}) (interface{}, error) {
	// get field
	field := Get(doc, path)

	// increment field
	switch num := field.(type) {
	case int32:
		switch inc := increment.(type) {
		case int32:
			field = num + inc
		case int64:
			field = int64(num) + inc
		case float64:
			field = float64(num) + inc
		default:
			return nil, fmt.Errorf("increment is not a number")
		}
	case int64:
		switch inc := increment.(type) {
		case int32:
			field = num + int64(inc)
		case int64:
			field = num + inc
		case float64:
			field = float64(num) + inc
		default:
			return nil, fmt.Errorf("increment is not a number")
		}
	case float64:
		switch inc := increment.(type) {
		case int32:
			field = num + float64(inc)
		case int64:
			field = num + float64(inc)
		case float64:
			field = num + inc
		default:
			return nil, fmt.Errorf("increment is not a number")
		}
	case MissingType:
		switch inc := increment.(type) {
		case int32, int64, float64:
			field = inc
		default:
			return nil, fmt.Errorf("increment is not a number")
		}
	default:
		return nil, fmt.Errorf("incrementee %q is not a number", path)
	}

	// update field
	_, err := Put(doc, path, field, false)
	if err != nil {
		return nil, err
	}

	return field, nil
}

// Multiply will multiply the multiplier with the value at the location in the
// document specified by path and return the new value. If the value is missing,
// a zero is added to the document. The type of the field may be changed as part
// of the operation.
func Multiply(doc Doc, path string, multiplier interface{}) (interface{}, error) {
	// get field
	field := Get(doc, path)

	// multiply field
	switch num := field.(type) {
	case int32:
		switch mul := multiplier.(type) {
		case int32:
			field = num * mul
		case int64:
			field = int64(num) * mul
		case float64:
			field = float64(num) * mul
		default:
			return nil, fmt.Errorf("multiplier is not a number")
		}
	case int64:
		switch mul := multiplier.(type) {
		case int32:
			field = num * int64(mul)
		case int64:
			field = num * mul
		case float64:
			field = float64(num) * mul
		default:
			return nil, fmt.Errorf("multiplier is not a number")
		}
	case float64:
		switch mul := multiplier.(type) {
		case int32:
			field = num * float64(mul)
		case int64:
			field = num * float64(mul)
		case float64:
			field = num * mul
		default:
			return nil, fmt.Errorf("multiplier is not a number")
		}
	case MissingType:
		switch multiplier.(type) {
		case int32:
			field = int32(0)
		case int64:
			field = int64(0)
		case float64:
			field = float64(0)
		default:
			return nil, fmt.Errorf("multiplier is not a number")
		}
	default:
		return nil, fmt.Errorf("multiplicand %q is not a number", path)
	}

	// update field
	_, err := Put(doc, path, field, false)
	if err != nil {
		return nil, err
	}

	return field, nil
}

// Push will add the value to the array at the location in the document
// specified by path and return the new value. If the value is missing, the
// value is added to a new array.
func Push(doc Doc, path string, value interface{}) (interface{}, error) {
	// get field
	field := Get(doc, path)

	// push field
	switch val := field.(type) {
	case bson.A:
		field = append(val, value)
	case MissingType:
		field = bson.A{value}
	default:
		return field, nil
	}

	// update field
	_, err := Put(doc, path, field, false)
	if err != nil {
		return nil, err
	}

	return field, nil
}
