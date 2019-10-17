package bsoncmp

import (
	"fmt"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TODO: Support all types from primitive package.

type Type int

const (
	MinKey Type = iota
	Null
	Number
	String
	Object
	Array
	Binary
	ObjectID
	Boolean
	Date
	Timestamp
	Regex
	MaxKey
)

func Inspect(v interface{}) (Type, error) {
	switch v.(type) {
	case nil:
		return Null, nil
	case int32, int64, float64, primitive.Decimal128:
		return Number, nil
	case string:
		return String, nil
	case bson.M, bson.D:
		return Object, nil
	case bson.A:
		return Array, nil
	case []byte:
		return Binary, nil
	case primitive.ObjectID:
		return ObjectID, nil
	case bool:
		return Boolean, nil
	case time.Time:
		return Date, nil
	case primitive.Timestamp:
		return Timestamp, nil
	case primitive.Regex:
		return Regex, nil
	}

	return 0, fmt.Errorf("inspect: unknown type %q", reflect.TypeOf(v).String())
}
