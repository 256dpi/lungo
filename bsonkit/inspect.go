package bsonkit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

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

// Type2Alias is a map from BSON types to their alias.
var Type2Alias = map[bsontype.Type]string{
	bsontype.Double:           "double",
	bsontype.String:           "string",
	bsontype.EmbeddedDocument: "object",
	bsontype.Array:            "array",
	bsontype.Binary:           "binData",
	bsontype.Undefined:        "undefined",
	bsontype.ObjectID:         "objectId",
	bsontype.Boolean:          "bool",
	bsontype.DateTime:         "date",
	bsontype.Null:             "null",
	bsontype.Regex:            "regex",
	bsontype.DBPointer:        "dbPointer",
	bsontype.JavaScript:       "javascript",
	bsontype.Symbol:           "symbol",
	bsontype.CodeWithScope:    "javascriptWithScope",
	bsontype.Int32:            "int",
	bsontype.Timestamp:        "timestamp",
	bsontype.Int64:            "long",
	bsontype.Decimal128:       "decimal",
	bsontype.MinKey:           "minKey",
	bsontype.MaxKey:           "maxKey",
}

// Alias2Type is a map from BSON type aliases to BSON types.
var Alias2Type = map[string]bsontype.Type{}

// Number2Type is a map from BSON type numbers to BSON types.
var Number2Type = map[byte]bsontype.Type{}

func init() {
	// fill aliases and number maps
	for t, a := range Type2Alias {
		Alias2Type[a] = t
		Number2Type[byte(t)] = t
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
	case primitive.Decimal128:
		return Number, bsontype.Decimal128
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
