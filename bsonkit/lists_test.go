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
}

func TestSort(t *testing.T) {
	a1 := Convert(bson.M{"a": "1"})
	a2 := Convert(bson.M{"a": "2"})
	a3 := Convert(bson.M{"a": "3"})

	list := Sort(List{a3, a1, a2}, "a", false)
	assert.Equal(t, List{a1, a2, a3}, list)

	list = Sort(List{a3, a1, a2}, "a", true)
	assert.Equal(t, List{a3, a2, a1}, list)

	b1 := Convert(bson.M{"b": true})
	b2 := Convert(bson.M{"b": "foo"})
	b3 := Convert(bson.M{"b": 4.2})

	list = Sort(List{b1, b2, b3}, "b", false)
	assert.Equal(t, List{b3, b2, b1}, list)

	list = Sort(List{b1, b2, b3}, "b", true)
	assert.Equal(t, List{b1, b2, b3}, list)
}

func TestCollect(t *testing.T) {
	a1 := Convert(bson.M{"a": "1"})
	a2 := Convert(bson.M{"a": "2"})
	a3 := Convert(bson.M{"a": "3"})
	b1 := Convert(bson.M{"b": "3"})

	res := Collect(List{a1, a2, b1, a3}, "a", false, false)
	assert.Equal(t, []interface{}{"1", "2", Missing, "3"}, res)

	res = Collect(List{a1, b1, a2, a3}, "a", true, false)
	assert.Equal(t, []interface{}{"1", "2", "3"}, res)

	res = Collect(List{a1, b1, a2, a1, a3, a1}, "a", true, true)
	assert.Equal(t, []interface{}{"1", "2", "3"}, res)
}
