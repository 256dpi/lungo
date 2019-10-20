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

func TestSort(t *testing.T) {
	a1 := Convert(bson.M{"a": "1", "b": true})
	a2 := Convert(bson.M{"a": "2", "b": false})
	a3 := Convert(bson.M{"a": "3", "b": true})

	// sort forwards single
	list := List{a3, a1, a2}
	Sort(list, []SortOrder{
		{Path: "a", Reverse: false},
	})
	assert.Equal(t, List{a1, a2, a3}, list)

	// sort backwards single
	list = List{a3, a1, a2}
	Sort(list, []SortOrder{
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a3, a2, a1}, list)

	// sort forwards multiple
	list = List{a3, a1, a2}
	Sort(list, []SortOrder{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: false},
	})
	assert.Equal(t, List{a2, a1, a3}, list)

	// sort backwards multiple
	list = List{a3, a1, a2}
	Sort(list, []SortOrder{
		{Path: "b", Reverse: true},
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a3, a1, a2}, list)

	// sort mixed
	list = List{a3, a1, a2}
	Sort(list, []SortOrder{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a2, a3, a1}, list)
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
