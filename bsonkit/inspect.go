package bsonkit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TODO: Add support for decimal128 type.

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
	case nil, primitive.Null, MissingType:
		return Null
	case int32, int64, float64:
		return Number
	case string:
		return String
	case bson.D:
		return Object
	case bson.A:
		return Array
	case primitive.Binary:
		return Binary
	case primitive.ObjectID:
		return ObjectID
	case bool:
		return Boolean
	case primitive.DateTime:
		return Date
	case primitive.Timestamp:
		return Timestamp
	case primitive.Regex:
		return Regex
	default:
		panic(fmt.Sprintf("bsonkit: cannot inspect: %T", v))
	}
}
