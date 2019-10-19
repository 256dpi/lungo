package bsonkit

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

type MissingType struct{}

var Missing = MissingType{}

func Get(doc Doc, path string) interface{} {
	return get(doc, strings.Split(path, "."))
}

func get(doc Doc, path []string) interface{} {
	// search for element
	for _, el := range *doc {
		if el.Key == path[0] {
			if len(path) == 1 {
				return el.Value
			}

			// check if doc
			if d, ok := el.Value.(bson.D); ok {
				return get(&d, path[1:])
			}

			return Missing
		}
	}

	return Missing
}

func Set(doc Doc, path string, value interface{}, prepend bool) error {
	return set(doc, strings.Split(path, "."), value, prepend)
}

func set(doc Doc, path []string, value interface{}, prepend bool) error {
	// search for element
	for i, el := range *doc {
		if el.Key == path[0] {
			// replace value
			if len(path) == 1 {
				(*doc)[i].Value = value
				return nil
			}

			// check if doc
			if d, ok := el.Value.(bson.D); ok {
				err := set(&d, path[1:], value, prepend)
				if err != nil {
					return err
				}

				// update value
				(*doc)[i].Value = d

				return nil
			}

			return fmt.Errorf("set: cannot set field in %+v", el.Value)
		}
	}

	// add intermediary element
	if len(path) > 1 {
		// prepare object
		d := bson.D{}
		err := set(&d, path[1:], value, prepend)
		if err != nil {
			return err
		}

		// add object
		e := bson.E{Key: path[0], Value: d}
		if prepend {
			*doc = append(bson.D{e}, *doc...)
		} else {
			*doc = append(*doc, e)
		}

		return nil
	}

	// add final element
	e := bson.E{Key: path[0], Value: value}
	if prepend {
		*doc = append(bson.D{e}, *doc...)
	} else {
		*doc = append(*doc, e)
	}

	return nil
}

func Unset(doc Doc, path string) error {
	return unset(doc, strings.Split(path, "."))
}

func unset(doc Doc, path []string) error {
	// search for element
	for i, el := range *doc {
		if el.Key == path[0] {
			// delete element
			if len(path) == 1 {
				*doc = append((*doc)[:i], (*doc)[i+1:]...)
				return nil
			}

			// check if doc
			if d, ok := el.Value.(bson.D); ok {
				err := unset(&d, path[1:])
				if err != nil {
					return err
				}

				// update value
				(*doc)[i].Value = d

				return nil
			}

			return fmt.Errorf("unset: cannot unset field in %+v", el.Value)
		}
	}

	return nil
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
			return fmt.Errorf("increment: expected number")
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
			return fmt.Errorf("increment: expected number")
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
			return fmt.Errorf("increment: expected number")
		}
	case MissingType:
		switch inc := increment.(type) {
		case int32, int64, float64:
			field = inc
		default:
			return fmt.Errorf("increment: expected number")
		}
	default:
		return fmt.Errorf("increment: field is not a number")
	}

	// update field
	err := Set(doc, path, field, false)
	if err != nil {
		return err
	}

	return nil
}

func Multiply(doc Doc, path string, multiplicand interface{}) error {
	// get field
	field := Get(doc, path)

	// multiply field
	switch num := field.(type) {
	case int32:
		switch mul := multiplicand.(type) {
		case int32:
			field = num * mul
		case int64:
			field = num * int32(mul)
		case float64:
			field = num * int32(mul)
		default:
			return fmt.Errorf("multiply: expected number")
		}
	case int64:
		switch mul := multiplicand.(type) {
		case int32:
			field = num * int64(mul)
		case int64:
			field = num * mul
		case float64:
			field = num * int64(mul)
		default:
			return fmt.Errorf("multiply: expected number")
		}
	case float64:
		switch mul := multiplicand.(type) {
		case int32:
			field = num * float64(mul)
		case int64:
			field = num * float64(mul)
		case float64:
			field = num * mul
		default:
			return fmt.Errorf("multiply: expected number")
		}
	case MissingType:
		switch multiplicand.(type) {
		case int32:
			field = int32(0)
		case int64:
			field = int64(0)
		case float64:
			field = float64(0)
		default:
			return fmt.Errorf("multiply: expected number")
		}
	default:
		return fmt.Errorf("multiply: field is not a number")
	}

	// update field
	err := Set(doc, path, field, false)
	if err != nil {
		return err
	}

	return nil
}
