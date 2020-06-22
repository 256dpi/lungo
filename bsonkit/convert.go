package bsonkit

import (
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MustConvert will call Convert and panic on errors.
func MustConvert(v interface{}) Doc {
	doc, err := Convert(v)
	if err != nil {
		panic("bsonkit: " + err.Error())
	}

	return doc
}

// Convert will convert the provided value to a document. The value is expected
// to be a bson.M or bson.D composed of standard types.
func Convert(v interface{}) (Doc, error) {
	// convert value
	res, err := ConvertValue(v)
	if err != nil {
		return nil, err
	}

	// check value
	doc, ok := res.(bson.D)
	if !ok {
		return nil, fmt.Errorf(`expected conversion to result in a "bson.D"`)
	}

	return &doc, nil
}

// MustConvertList will call ConvertList and panic on errors.
func MustConvertList(v interface{}) List {
	list, err := ConvertList(v)
	if err != nil {
		panic("bsonkit: " + err.Error())
	}

	return list
}

// ConvertList will convert an array to a list. The value is expected to be a
// bson.A of bson.M or bson.D elements composed of standard types.
func ConvertList(v interface{}) (List, error) {
	// convert value
	doc, err := ConvertValue(v)
	if err != nil {
		return nil, err
	}

	// check array
	array, ok := doc.(bson.A)
	if !ok {
		return nil, fmt.Errorf(`expected array`)
	}

	// build list
	list := make(List, 0, len(array))
	for _, item := range array {
		doc, ok := item.(bson.D)
		if !ok {
			return nil, fmt.Errorf(`expected array of documents`)
		}
		list = append(list, &doc)
	}

	return list, nil
}

// MustConvertValue will call ConvertValue and panic on errors.
func MustConvertValue(v interface{}) interface{} {
	// convert value
	res, err := ConvertValue(v)
	if err != nil {
		panic(err)
	}

	return res
}

// ConvertValue will convert the provided type to a standard type.
func ConvertValue(v interface{}) (interface{}, error) {
	// convert recursively
	var err error
	switch value := v.(type) {
	case bson.M:
		return convertMap(value)
	case map[string]interface{}:
		return convertMap(value)
	case bson.A:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i], err = ConvertValue(item)
			if err != nil {
				return nil, err
			}
		}
		return a, nil
	case []interface{}:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i], err = ConvertValue(item)
			if err != nil {
				return nil, err
			}
		}
		return a, nil
	case []string:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = item
		}
		return a, nil
	case []bson.M:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i], err = ConvertValue(item)
			if err != nil {
				return nil, err
			}
		}
		return a, nil
	case bson.D:
		d := make(bson.D, len(value))
		for i, item := range value {
			d[i].Key = item.Key
			d[i].Value, err = ConvertValue(item.Value)
			if err != nil {
				return nil, err
			}
		}
		return d, nil
	case []bson.D:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i], err = ConvertValue(item)
			if err != nil {
				return nil, err
			}
		}
		return a, nil
	case []primitive.ObjectID:
		a := make(bson.A, len(value))
		for i, item := range value {
			a[i] = item
		}
		return a, nil
	case nil, int32, int64, float64, string, bool:
		return value, nil
	case int:
		return int64(value), nil
	case primitive.Null, primitive.ObjectID, primitive.DateTime,
		primitive.Timestamp, primitive.Regex, primitive.Binary:
		return value, nil
	case *primitive.ObjectID:
		if value != nil {
			return *value, nil
		}
		return nil, nil
	case time.Time:
		return primitive.NewDateTimeFromTime(value.UTC()), nil
	case *time.Time:
		if value != nil {
			return primitive.NewDateTimeFromTime(value.UTC()), nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func convertMap(m bson.M) (bson.D, error) {
	// prepare document
	d := make(bson.D, 0, len(m))

	// copy keys
	for key, field := range m {
		v, err := ConvertValue(field)
		if err != nil {
			return nil, err
		}

		d = append(d, bson.E{
			Key:   key,
			Value: v,
		})
	}

	// sort document
	sort.Slice(d, func(i, j int) bool {
		return d[i].Key < d[j].Key
	})

	return d, nil
}
