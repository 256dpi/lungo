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
	}{
		{in: nil, vc: Null, vt: bsontype.Null},
		{in: primitive.Null{}, vc: Null, vt: bsontype.Null},
		{in: int32(42), vc: Number, vt: bsontype.Int32},
		{in: int64(42), vc: Number, vt: bsontype.Int64},
		{in: 4.2, vc: Number, vt: bsontype.Double},
		{in: "", vc: String, vt: bsontype.String},
		{in: "foo", vc: String, vt: bsontype.String},
		{in: bson.D{}, vc: Document, vt: bsontype.EmbeddedDocument},
		{in: bson.A{}, vc: Array, vt: bsontype.Array},
		{in: primitive.Binary{}, vc: Binary, vt: bsontype.Binary},
		{in: primitive.NewObjectID(), vc: ObjectID, vt: bsontype.ObjectID},
		{in: true, vc: Boolean, vt: bsontype.Boolean},
		{in: false, vc: Boolean, vt: bsontype.Boolean},
		{in: primitive.DateTime(1570729020000), vc: Date, vt: bsontype.DateTime},
		{in: primitive.Timestamp{}, vc: Timestamp, vt: bsontype.Timestamp},
		{in: primitive.Regex{}, vc: Regex, vt: bsontype.Regex},
	}

	for i, item := range table {
		vc, vt := Inspect(item.in)
		assert.Equal(t, item.vc, vc, i)
		assert.Equal(t, item.vt, vt, i)
	}
}
