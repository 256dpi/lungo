package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestInspect(t *testing.T) {
	table := []struct {
		in interface{}
		vc Class
		vt bsontype.Type
		al string
	}{
		{
			in: nil,
			vc: Null,
			vt: bsontype.Null,
			al: "null",
		},
		{
			in: primitive.Null{},
			vc: Null,
			vt: bsontype.Null,
			al: "null",
		},
		{
			in: int32(42),
			vc: Number,
			vt: bsontype.Int32,
			al: "int",
		},
		{
			in: int64(42),
			vc: Number,
			vt: bsontype.Int64,
			al: "long",
		},
		{
			in: 4.2,
			vc: Number,
			vt: bsontype.Double,
			al: "double",
		},
		{
			in: "",
			vc: String,
			vt: bsontype.String,
			al: "string",
		},
		{
			in: "foo",
			vc: String,
			vt: bsontype.String,
			al: "string",
		},
		{
			in: bson.D{},
			vc: Document,
			vt: bsontype.EmbeddedDocument,
			al: "object",
		},
		{
			in: bson.A{},
			vc: Array,
			vt: bsontype.Array,
			al: "array",
		},
		{
			in: primitive.Binary{},
			vc: Binary,
			vt: bsontype.Binary,
			al: "binData",
		},
		{
			in: primitive.NewObjectID(),
			vc: ObjectID,
			vt: bsontype.ObjectID,
			al: "objectId",
		},
		{
			in: true,
			vc: Boolean,
			vt: bsontype.Boolean,
			al: "bool",
		},
		{
			in: false,
			vc: Boolean,
			vt: bsontype.Boolean,
			al: "bool",
		},
		{
			in: primitive.DateTime(1570729020000),
			vc: Date,
			vt: bsontype.DateTime,
			al: "date",
		},
		{
			in: primitive.Timestamp{},
			vc: Timestamp,
			vt: bsontype.Timestamp,
			al: "timestamp",
		},
		{
			in: primitive.Regex{},
			vc: Regex,
			vt: bsontype.Regex,
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
