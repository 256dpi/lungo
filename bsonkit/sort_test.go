package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSort(t *testing.T) {
	a1 := MustConvert(bson.M{"a": "1", "b": true})
	a2 := MustConvert(bson.M{"a": "2", "b": false})
	a3 := MustConvert(bson.M{"a": "3", "b": true})

	// sort forwards single
	list := List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "a", Reverse: false},
	})
	assert.Equal(t, List{a1, a2, a3}, list)

	// sort backwards single
	list = List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a3, a2, a1}, list)

	// sort forwards multiple
	list = List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: false},
	})
	assert.Equal(t, List{a2, a1, a3}, list)

	// sort backwards multiple
	list = List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: true},
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a3, a1, a2}, list)

	// sort mixed
	list = List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a2, a3, a1}, list)
}

func TestSortArrayValuedField(t *testing.T) {
	// MongoDB sorts by the smallest element of an array for ascending and
	// the largest for descending; lexicographic comparison would produce a
	// different order for these inputs
	a := MustConvert(bson.M{"a": bson.A{int32(3), int32(1)}})
	b := MustConvert(bson.M{"a": bson.A{int32(2)}})
	c := MustConvert(bson.M{"a": bson.A{int32(5), int32(0)}})

	// ascending: min(a)=1, min(b)=2, min(c)=0 → c, a, b
	list := List{a, b, c}
	Sort(list, []Column{{Path: "a", Reverse: false}})
	assert.Equal(t, List{c, a, b}, list)

	// descending: max(a)=3, max(b)=2, max(c)=5 → c, a, b
	list = List{a, b, c}
	Sort(list, []Column{{Path: "a", Reverse: true}})
	assert.Equal(t, List{c, a, b}, list)

	// scalar mixed with an array: sort key for the array is min/max of the
	// elements; the scalar compares as itself
	scalar := MustConvert(bson.M{"a": int32(2)})
	list = List{a, scalar}
	Sort(list, []Column{{Path: "a", Reverse: false}})
	// min(a)=1 < scalar=2 → a, scalar
	assert.Equal(t, List{a, scalar}, list)
	list = List{a, scalar}
	Sort(list, []Column{{Path: "a", Reverse: true}})
	// max(a)=3 > scalar=2 → a, scalar
	assert.Equal(t, List{a, scalar}, list)
}

func TestSortStable(t *testing.T) {
	// documents with equal column values must retain their input (insertion)
	// order, matching MongoDB's stable-sort semantics
	a1 := MustConvert(bson.M{"a": "1", "b": true})
	a2 := MustConvert(bson.M{"a": "2", "b": false})
	a3 := MustConvert(bson.M{"a": "2", "b": false})
	a4 := MustConvert(bson.M{"a": "3", "b": true})

	// sort forwards single — a2 and a3 tie on "a"; insertion order has a3
	// before a2, which must be preserved
	list := List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "a", Reverse: false},
	})
	assert.Equal(t, List{a1, a3, a2, a4}, list)

	// sort backwards single
	list = List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a4, a3, a2, a1}, list)

	// sort forwards multiple
	list = List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: false},
	})
	assert.Equal(t, List{a3, a2, a1, a4}, list)

	// sort backwards multiple
	list = List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: true},
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a4, a1, a3, a2}, list)

	// sort mixed
	list = List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: true},
	})
	assert.Equal(t, List{a3, a2, a4, a1}, list)
}
