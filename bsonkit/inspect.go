package bsonkit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
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
var Type2Alias = map[bson.Type]string{
	bson.TypeDouble:           "double",
	bson.TypeString:           "string",
	bson.TypeEmbeddedDocument: "object",
	bson.TypeArray:            "array",
	bson.TypeBinary:           "binData",
	bson.TypeUndefined:        "undefined",
	bson.TypeObjectID:         "objectId",
	bson.TypeBoolean:          "bool",
	bson.TypeDateTime:         "date",
	bson.TypeNull:             "null",
	bson.TypeRegex:            "regex",
	bson.TypeDBPointer:        "dbPointer",
	bson.TypeJavaScript:       "javascript",
	bson.TypeSymbol:           "symbol",
	bson.TypeCodeWithScope:    "javascriptWithScope",
	bson.TypeInt32:            "int",
	bson.TypeTimestamp:        "timestamp",
	bson.TypeInt64:            "long",
	bson.TypeDecimal128:       "decimal",
	bson.TypeMinKey:           "minKey",
	bson.TypeMaxKey:           "maxKey",
}

// Alias2Type is a map from BSON type aliases to BSON types.
var Alias2Type = map[string]bson.Type{}

// Number2Type is a map from BSON type numbers to BSON types.
var Number2Type = map[byte]bson.Type{}

func init() {
	// fill aliases and number maps
	for t, a := range Type2Alias {
		Alias2Type[a] = t
		Number2Type[byte(t)] = t
	}
}

// Inspect wil return the BSON type class and concrete type of the specified
// value.
func Inspect(v interface{}) (Class, bson.Type) {
	switch v.(type) {
	case nil, bson.Null, MissingType:
		return Null, bson.TypeNull
	case int32:
		return Number, bson.TypeInt32
	case int64:
		return Number, bson.TypeInt64
	case float64:
		return Number, bson.TypeDouble
	case bson.Decimal128:
		return Number, bson.TypeDecimal128
	case string:
		return String, bson.TypeString
	case bson.D:
		return Document, bson.TypeEmbeddedDocument
	case bson.A:
		return Array, bson.TypeArray
	case bson.Binary:
		return Binary, bson.TypeBinary
	case bson.ObjectID:
		return ObjectID, bson.TypeObjectID
	case bool:
		return Boolean, bson.TypeBoolean
	case bson.DateTime:
		return Date, bson.TypeDateTime
	case bson.Timestamp:
		return Timestamp, bson.TypeTimestamp
	case bson.Regex:
		return Regex, bson.TypeRegex
	default:
		panic(fmt.Sprintf("bsonkit: cannot inspect: %T", v))
	}
}
