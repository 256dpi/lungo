package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestTransform(t *testing.T) {
	table := []struct {
		in  interface{}
		out interface{}
		err bool
	}{
		{
			in: bson.M{"foo": "bar"},
			out: bson.D{
				bson.E{Key: "foo", Value: "bar"},
			},
		},
		{
			in: bson.D{
				bson.E{Key: "foo", Value: "bar"},
			},
			out: bson.D{
				bson.E{Key: "foo", Value: "bar"},
			},
		},
		{
			in: bson.M{
				"foo": bson.A{
					bson.M{"bar": "baz"},
				},
			},
			out: bson.D{
				bson.E{Key: "foo", Value: bson.A{
					bson.D{
						bson.E{Key: "bar", Value: "baz"},
					},
				}},
			},
		},
		{
			in: struct{ Title string }{Title: "cool"},
			out: bson.D{
				bson.E{Key: "title", Value: "cool"},
			},
		},
		{
			in: bson.D{
				bson.E{Key: "true", Value: true},
				bson.E{Key: "false", Value: false},
				bson.E{Key: "int", Value: 42},
				bson.E{Key: "int8", Value: int8(42)},
				bson.E{Key: "int16", Value: int16(42)},
				bson.E{Key: "int32", Value: int32(42)},
				bson.E{Key: "int64", Value: int64(42)},
				bson.E{Key: "uint", Value: uint(42)},
				bson.E{Key: "uint8", Value: uint8(42)},
				bson.E{Key: "uint16", Value: uint16(42)},
				bson.E{Key: "uint32", Value: uint32(42)},
				bson.E{Key: "uint64", Value: uint64(42)},
				bson.E{Key: "float32", Value: float32(4.2)},
				bson.E{Key: "float64", Value: 4.2},
			},
			out: bson.D{
				bson.E{Key: "true", Value: true},
				bson.E{Key: "false", Value: false},
				bson.E{Key: "int", Value: int32(42)},
				bson.E{Key: "int8", Value: int32(42)},
				bson.E{Key: "int16", Value: int32(42)},
				bson.E{Key: "int32", Value: int32(42)},
				bson.E{Key: "int64", Value: int64(42)},
				bson.E{Key: "uint", Value: int64(42)},
				bson.E{Key: "uint8", Value: int32(42)},
				bson.E{Key: "uint16", Value: int32(42)},
				bson.E{Key: "uint32", Value: int64(42)},
				bson.E{Key: "uint64", Value: int64(42)},
				bson.E{Key: "float32", Value: 4.199999809265137},
				bson.E{Key: "float64", Value: 4.2},
			},
		},
	}

	for i, item := range table {
		res, err := Transform(item.in)
		assert.Equal(t, item.err, err != nil, i)
		assert.Equal(t, item.out, res, i)
	}
}
