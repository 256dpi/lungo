package bsonkit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TODO: Add support for decimal128 type.

// Class is describes the class of one or more BSON types.
type Class int

// The available BSON type classes.
const (
	Null Class = iota
	Number
	String
	Document
	Array
	Binary
	ObjectID
	Boolean
	Date
	Timestamp
	Regex
)

// TypeString is a map from BSON type strings to BSON types.
var TypeString = map[string]bsontype.Type{}

// TypeNumber is a map from BSON type numbers to BSON types.
var TypeNumber = map[byte]bsontype.Type{}

func init() {
	// prepare types
	types := []bsontype.Type{
		bsontype.Double,
		bsontype.String,
		bsontype.EmbeddedDocument,
		bsontype.Array,
		bsontype.Binary,
		bsontype.Undefined,
		bsontype.ObjectID,
		bsontype.Boolean,
		bsontype.DateTime,
		bsontype.Null,
		bsontype.Regex,
		bsontype.DBPointer,
		bsontype.JavaScript,
		bsontype.Symbol,
		bsontype.CodeWithScope,
		bsontype.Int32,
		bsontype.Timestamp,
		bsontype.Int64,
		bsontype.Decimal128,
		bsontype.MinKey,
		bsontype.MaxKey,
	}

	// fill maps
	for _, t := range types {
		TypeString[t.String()] = t
		TypeNumber[byte(t)] = t
	}
}

// Inspect wil return the BSON type class and concrete type of the specified
// value.
func Inspect(v interface{}) (Class, bsontype.Type) {
	switch v.(type) {
	case nil, primitive.Null, MissingType:
		return Null, bsontype.Null
	case int32:
		return Number, bsontype.Int32
	case int64:
		return Number, bsontype.Int64
	case float64:
		return Number, bsontype.Double
	case string:
		return String, bsontype.String
	case bson.D:
		return Document, bsontype.EmbeddedDocument
	case bson.A:
		return Array, bsontype.Array
	case primitive.Binary:
		return Binary, bsontype.Binary
	case primitive.ObjectID:
		return ObjectID, bsontype.ObjectID
	case bool:
		return Boolean, bsontype.Boolean
	case primitive.DateTime:
		return Date, bsontype.DateTime
	case primitive.Timestamp:
		return Timestamp, bsontype.Timestamp
	case primitive.Regex:
		return Regex, bsontype.Regex
	default:
		panic(fmt.Sprintf("bsonkit: cannot inspect: %T", v))
	}
}
