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
		t1 Type
		t2 bsontype.Type
	}{
		{in: nil, t1: Null, t2: bsontype.Null},
		{in: primitive.Null{}, t1: Null, t2: bsontype.Null},
		{in: int32(42), t1: Number, t2: bsontype.Int32},
		{in: int64(42), t1: Number, t2: bsontype.Int64},
		{in: 4.2, t1: Number, t2: bsontype.Double},
		{in: "", t1: String, t2: bsontype.String},
		{in: "foo", t1: String, t2: bsontype.String},
		{in: bson.D{}, t1: Object, t2: bsontype.EmbeddedDocument},
		{in: bson.A{}, t1: Array, t2: bsontype.Array},
		{in: primitive.Binary{}, t1: Binary, t2: bsontype.Binary},
		{in: primitive.NewObjectID(), t1: ObjectID, t2: bsontype.ObjectID},
		{in: true, t1: Boolean, t2: bsontype.Boolean},
		{in: false, t1: Boolean, t2: bsontype.Boolean},
		{in: primitive.DateTime(1570729020000), t1: Date, t2: bsontype.DateTime},
		{in: primitive.Timestamp{}, t1: Timestamp, t2: bsontype.Timestamp},
		{in: primitive.Regex{}, t1: Regex, t2: bsontype.Regex},
	}

	for i, item := range table {
		t1, t2 := Inspect(item.in)
		assert.Equal(t, item.t1, t1, i)
		assert.Equal(t, item.t2, t2, i)
	}
}
