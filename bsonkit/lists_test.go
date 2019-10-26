package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestSelect(t *testing.T) {
	a := &bson.D{}
	b := &bson.D{}

	// select all matching
	list := Select(List{a, nil, b, nil}, 0, func(doc Doc) (bool, bool) {
		return doc != nil, false
	})
	assert.Equal(t, List{a, b}, list)

	// select all matching with exit
	i := 0
	list = Select(List{a, nil, b, nil}, 0, func(doc Doc) (bool, bool) {
		i++
		return doc != nil, i > 1
	})
	assert.Equal(t, List{a}, list)

	// select all matching with limit
	list = Select(List{a, nil, b, nil}, 1, func(doc Doc) (bool, bool) {
		return doc != nil, false
	})
	assert.Equal(t, List{a}, list)
}

func TestCollect(t *testing.T) {
	a1 := Convert(bson.M{"a": "1"})
	a2 := Convert(bson.M{"a": "2"})
	a3 := Convert(bson.M{"a": "2"})
	b1 := Convert(bson.M{"b": "3"})

	// raw values
	res := Collect(List{a1, a2, b1, a3}, "a", false, false)
	assert.Equal(t, []interface{}{"1", "2", Missing, "2"}, res)

	// compact values
	res = Collect(List{a1, b1, a2, a3}, "a", true, false)
	assert.Equal(t, []interface{}{"1", "2", "2"}, res)

	// distinct values
	res = Collect(List{a1, b1, a2, a3}, "a", false, true)
	assert.Equal(t, []interface{}{Missing, "1", "2"}, res)

	// compact and distinct values
	res = Collect(List{a1, b1, a2, a1, a3, a1}, "a", true, true)
	assert.Equal(t, []interface{}{"1", "2"}, res)
}
