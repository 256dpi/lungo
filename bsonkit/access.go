package bsonkit

import (
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

type MissingType struct{}

var Missing = MissingType{}

var unsetValue interface{} = struct{}{}

func Get(doc Doc, path string) interface{} {
	value, _ := get(*doc, strings.Split(path, "."), false)
	return value
}

func All(doc Doc, path string, collect, flatten bool) (interface{}, bool) {
	// get value
	value, nested := get(*doc, strings.Split(path, "."), collect)
	if !flatten {
		return value, nested
	}

	// get array
	array, ok := value.(bson.A)
	if !ok {
		return value, nested
	}

	// prepare result
	result := make(bson.A, 0, len(array))

	// flatten array
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

func get(v interface{}, path []string, collect bool) (interface{}, bool) {
	// check path
	if len(path) == 0 {
		return v, false
	}

	// check if empty
	if path[0] == "" {
		return Missing, false
	}

	// get document field
	if doc, ok := v.(bson.D); ok {
		for _, el := range doc {
			if el.Key == path[0] {
				return get(el.Value, path[1:], collect)
			}
		}
	}

	// get array element
	if arr, ok := v.(bson.A); ok {
		// get indexed array element
		index, err := strconv.ParseInt(path[0], 10, 64)
		if err == nil && index >= 0 && index < int64(len(arr)) {
			return get(arr[index], path[1:], collect)
		}

		// collect value from embedded documents
		if collect {
			res := make(bson.A, 0, len(arr))
			for _, item := range arr {
				value, ok := get(item, path, collect)
				if value != Missing {
					if ok {
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

func Put(doc Doc, path string, value interface{}, prepend bool) error {
	ok := put(*doc, strings.Split(path, "."), value, prepend, func(v interface{}) {
		*doc = v.(bson.D)
	})
	if !ok {
		return fmt.Errorf("cannot put value at %s", path)
	}

	return nil
}

func Unset(doc Doc, path string) {
	_ = put(*doc, strings.Split(path, "."), unsetValue, false, func(v interface{}) {
		*doc = v.(bson.D)
	})
}

func put(v interface{}, path []string, value interface{}, prepend bool, set func(interface{})) bool {
	// check path
	if len(path) == 0 {
		set(value)
		return true
	}

	// check if empty
	if path[0] == "" {
		return false
	}

	// put document field
	if doc, ok := v.(bson.D); ok {
		for i, el := range doc {
			if el.Key == path[0] {
				return put(doc[i].Value, path[1:], value, prepend, func(v interface{}) {
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
			return true
		}

		// capture value
		e := bson.E{Key: path[0]}
		ok := put(Missing, path[1:], value, prepend, func(v interface{}) {
			e.Value = v
		})
		if !ok {
			return false
		}

		// set appended/prepended document
		if prepend {
			set(append(bson.D{e}, doc...))
		} else {
			set(append(doc, e))
		}

		return true
	}

	// put array element
	if arr, ok := v.(bson.A); ok {
		index, err := strconv.Atoi(path[0])
		if err != nil || index < 0 {
			return false
		}

		// update existing element
		if index < len(arr) {
			return put(arr[index], path[1:], value, prepend, func(v interface{}) {
				if v == unsetValue {
					arr[index] = nil
				} else {
					arr[index] = v
				}
			})
		}

		// check if unset
		if value == unsetValue {
			return true
		}

		// fill with nil elements
		for i := len(arr); i < index+1; i++ {
			arr = append(arr, nil)
		}

		// put in last element
		ok := put(Missing, path[1:], value, prepend, func(v interface{}) {
			arr[index] = v
		})
		if !ok {
			return false
		}

		// set array
		set(arr)

		return true
	}

	// check if unset
	if value == unsetValue {
		return true
	}

	// put new document
	if v == Missing {
		// capture value
		e := bson.E{Key: path[0]}
		ok := put(Missing, path[1:], value, prepend, func(v interface{}) {
			e.Value = v
		})
		if !ok {
			return false
		}

		// set document
		set(bson.D{e})

		return true
	}

	return false
}

func Increment(doc Doc, path string, increment interface{}) error {
	// get field
	field := Get(doc, path)

	// increment field
	switch num := field.(type) {
	case int32:
		switch inc := increment.(type) {
		case int32:
			field = num + inc
		case int64:
			field = num + int32(inc)
		case float64:
			field = num + int32(inc)
		default:
			return fmt.Errorf("increment is not a number")
		}
	case int64:
		switch inc := increment.(type) {
		case int32:
			field = num + int64(inc)
		case int64:
			field = num + inc
		case float64:
			field = num + int64(inc)
		default:
			return fmt.Errorf("increment is not a number")
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
			return fmt.Errorf("increment is not a number")
		}
	case MissingType:
		switch inc := increment.(type) {
		case int32, int64, float64:
			field = inc
		default:
			return fmt.Errorf("increment is not a number")
		}
	default:
		return fmt.Errorf("incrementee %q is not a number", path)
	}

	// update field
	err := Put(doc, path, field, false)
	if err != nil {
		return err
	}

	return nil
}

func Multiply(doc Doc, path string, multiplier interface{}) error {
	// get field
	field := Get(doc, path)

	// multiply field
	switch num := field.(type) {
	case int32:
		switch mul := multiplier.(type) {
		case int32:
			field = num * mul
		case int64:
			field = num * int32(mul)
		case float64:
			field = num * int32(mul)
		default:
			return fmt.Errorf("multiplier is not a number")
		}
	case int64:
		switch mul := multiplier.(type) {
		case int32:
			field = num * int64(mul)
		case int64:
			field = num * mul
		case float64:
			field = num * int64(mul)
		default:
			return fmt.Errorf("multiplier is not a number")
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
			return fmt.Errorf("multiplier is not a number")
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
			return fmt.Errorf("multiplier is not a number")
		}
	default:
		return fmt.Errorf("multiplicand %q is not a number", path)
	}

	// update field
	err := Put(doc, path, field, false)
	if err != nil {
		return err
	}

	return nil
}
