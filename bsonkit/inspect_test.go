package bsonkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestInspect(t *testing.T) {
	table := []struct {
		in  interface{}
		out Type
	}{
		{in: nil, out: Null},
		{in: primitive.Null{}, out: Null},
		{in: int32(42), out: Number},
		{in: int64(42), out: Number},
		{in: 4.2, out: Number},
		{in: "", out: String},
		{in: "foo", out: String},
		{in: bson.D{}, out: Object},
		{in: bson.A{}, out: Array},
		{in: []byte{4, 2}, out: Binary},
		{in: primitive.NewObjectID(), out: ObjectID},
		{in: true, out: Boolean},
		{in: false, out: Boolean},
		{in: time.Now(), out: Date},
		{in: primitive.Timestamp{}, out: Timestamp},
		{in: primitive.Regex{}, out: Regex},
	}

	for i, item := range table {
		assert.Equal(t, item.out, Inspect(item.in), i)
	}
}
