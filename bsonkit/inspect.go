package bsonkit

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
	Null Type = iota
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
)

func Inspect(v interface{}) (Type, error) {
	switch v.(type) {
	case nil:
		return Null, nil
	case int32, int64, float64:
		return Number, nil
	case string:
		return String, nil
	case bson.D:
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
