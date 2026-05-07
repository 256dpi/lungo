package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestInspect(t *testing.T) {
	table := []struct {
		in interface{}
		vc Class
		vt bson.Type
		al string
	}{
		{
			in: nil,
			vc: Null,
			vt: bson.TypeNull,
			al: "null",
		},
		{
			in: bson.Null{},
			vc: Null,
			vt: bson.TypeNull,
			al: "null",
		},
		{
			in: int32(42),
			vc: Number,
			vt: bson.TypeInt32,
			al: "int",
		},
		{
			in: int64(42),
			vc: Number,
			vt: bson.TypeInt64,
			al: "long",
		},
		{
			in: 4.2,
			vc: Number,
			vt: bson.TypeDouble,
			al: "double",
		},
		{
			in: bson.NewDecimal128(1, 1),
			vc: Number,
			vt: bson.TypeDecimal128,
			al: "decimal",
		},
		{
			in: "",
			vc: String,
			vt: bson.TypeString,
			al: "string",
		},
		{
			in: "foo",
			vc: String,
			vt: bson.TypeString,
			al: "string",
		},
		{
			in: bson.D{},
			vc: Document,
			vt: bson.TypeEmbeddedDocument,
			al: "object",
		},
		{
			in: bson.A{},
			vc: Array,
			vt: bson.TypeArray,
			al: "array",
		},
		{
			in: bson.Binary{},
			vc: Binary,
			vt: bson.TypeBinary,
			al: "binData",
		},
		{
			in: bson.NewObjectID(),
			vc: ObjectID,
			vt: bson.TypeObjectID,
			al: "objectId",
		},
		{
			in: true,
			vc: Boolean,
			vt: bson.TypeBoolean,
			al: "bool",
		},
		{
			in: false,
			vc: Boolean,
			vt: bson.TypeBoolean,
			al: "bool",
		},
		{
			in: bson.DateTime(1570729020000),
			vc: Date,
			vt: bson.TypeDateTime,
			al: "date",
		},
		{
			in: bson.Timestamp{},
			vc: Timestamp,
			vt: bson.TypeTimestamp,
			al: "timestamp",
		},
		{
			in: bson.Regex{},
			vc: Regex,
			vt: bson.TypeRegex,
			al: "regex",
		},
	}

	for i, item := range table {
		vc, vt := Inspect(item.in)
		assert.Equal(t, item.vc, vc, i)
		assert.Equal(t, item.vt, vt, i)
		assert.Equal(t, item.al, Type2Alias[vt], i)
	}
}
