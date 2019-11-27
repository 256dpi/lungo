package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestSort(t *testing.T) {
	a1 := Convert(bson.M{"a": "1", "b": true})
	a2 := Convert(bson.M{"a": "2", "b": false})
	a3 := Convert(bson.M{"a": "3", "b": true})

	// sort forwards single
	list := List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "a", Reverse: false},
	}, false)
	assert.Equal(t, List{a1, a2, a3}, list)

	// sort backwards single
	list = List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "a", Reverse: true},
	}, false)
	assert.Equal(t, List{a3, a2, a1}, list)

	// sort forwards multiple
	list = List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: false},
	}, false)
	assert.Equal(t, List{a2, a1, a3}, list)

	// sort backwards multiple
	list = List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: true},
		{Path: "a", Reverse: true},
	}, false)
	assert.Equal(t, List{a3, a1, a2}, list)

	// sort mixed
	list = List{a3, a1, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: true},
	}, false)
	assert.Equal(t, List{a2, a3, a1}, list)
}

func TestSortIdentity(t *testing.T) {
	a1 := Convert(bson.M{"a": "1", "b": true})
	a2 := Convert(bson.M{"a": "2", "b": false})
	a3 := Convert(bson.M{"a": "2", "b": false})
	a4 := Convert(bson.M{"a": "3", "b": true})

	// sort forwards single
	list := List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "a", Reverse: false},
	}, true)
	assert.Equal(t, List{a1, a2, a3, a4}, list)

	// sort backwards single
	list = List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "a", Reverse: true},
	}, true)
	assert.Equal(t, List{a4, a3, a2, a1}, list)

	// sort forwards multiple
	list = List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: false},
	}, true)
	assert.Equal(t, List{a2, a3, a1, a4}, list)

	// sort backwards multiple
	list = List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: true},
		{Path: "a", Reverse: true},
	}, true)
	assert.Equal(t, List{a4, a1, a2, a3}, list)

	// sort mixed
	list = List{a3, a1, a4, a2}
	Sort(list, []Column{
		{Path: "b", Reverse: false},
		{Path: "a", Reverse: true},
	}, true)
	assert.Equal(t, List{a2, a3, a4, a1}, list)
}
