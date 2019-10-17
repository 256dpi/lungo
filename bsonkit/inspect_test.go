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
		err string
	}{
		{in: nil, out: Null},
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

		{in: 42, err: `inspect: unknown type "int"`},
		{in: struct{}{}, err: `inspect: unknown type "struct {}"`},
	}

	for i, item := range table {
		res, err := Inspect(item.in)
		if err != nil {
			assert.Equal(t, item.err, err.Error(), i)
		} else {
			assert.Equal(t, "", item.err, i)
		}
		assert.Equal(t, item.out, res, i)
	}
}
