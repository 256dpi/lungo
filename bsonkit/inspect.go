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

func Inspect(v interface{}) Type {
	switch v.(type) {
	case nil, primitive.Null:
		return Null
	case int32, int64, float64:
		return Number
	case string:
		return String
	case bson.D:
		return Object
	case bson.A:
		return Array
	case []byte:
		return Binary
	case primitive.ObjectID:
		return ObjectID
	case bool:
		return Boolean
	case time.Time:
		return Date
	case primitive.Timestamp:
		return Timestamp
	case primitive.Regex:
		return Regex
	default:
		panic(fmt.Sprintf("inspect: unsupported type: %q", reflect.TypeOf(v).String()))
	}
}
