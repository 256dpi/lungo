package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestSet(t *testing.T) {
	d1 := &bson.D{}
	d2 := &bson.D{}

	set := NewSet(nil)

	ok := set.Add(d1)
	assert.True(t, ok)
	assert.Equal(t, &Set{
		List: List{d1},
		Index: map[Doc]int{
			d1: 0,
		},
	}, set)

	ok = set.Add(d1)
	assert.False(t, ok)
	assert.Equal(t, &Set{
		List: List{d1},
		Index: map[Doc]int{
			d1: 0,
		},
	}, set)

	ok = set.Add(d2)
	assert.True(t, ok)
	assert.Equal(t, &Set{
		List: List{d1, d2},
		Index: map[Doc]int{
			d1: 0,
			d2: 1,
		},
	}, set)

	ok = set.Remove(d1)
	assert.True(t, ok)
	assert.Equal(t, &Set{
		List: List{d2},
		Index: map[Doc]int{
			d2: 0,
		},
	}, set)

	ok = set.Add(d1)
	assert.True(t, ok)
	assert.Equal(t, &Set{
		List: List{d2, d1},
		Index: map[Doc]int{
			d2: 0,
			d1: 1,
		},
	}, set)

	ok = set.Remove(d2)
	assert.True(t, ok)
	assert.Equal(t, &Set{
		List: List{d1},
		Index: map[Doc]int{
			d1: 0,
		},
	}, set)

	ok = set.Remove(d1)
	assert.True(t, ok)
	assert.Equal(t, &Set{
		List:  List{},
		Index: map[Doc]int{},
	}, set)

	ok = set.Remove(d1)
	assert.False(t, ok)
	assert.Equal(t, &Set{
		List:  List{},
		Index: map[Doc]int{},
	}, set)
}

func TestSetReplace(t *testing.T) {
	d1 := &bson.D{}
	d2 := &bson.D{}
	d3 := &bson.D{}
	d4 := &bson.D{}

	set := NewSet(List{d1, d2, d3})

	ok := set.Replace(d2, d4)
	assert.True(t, ok)
	assert.Equal(t, &Set{
		List: List{d1, d4, d3},
		Index: map[Doc]int{
			d1: 0,
			d4: 1,
			d3: 2,
		},
	}, set)

	ok = set.Replace(d2, d4)
	assert.False(t, ok)
	assert.Equal(t, &Set{
		List: List{d1, d4, d3},
		Index: map[Doc]int{
			d1: 0,
			d4: 1,
			d3: 2,
		},
	}, set)

	ok = set.Replace(d1, d3)
	assert.False(t, ok)
	assert.Equal(t, &Set{
		List: List{d1, d4, d3},
		Index: map[Doc]int{
			d1: 0,
			d4: 1,
			d3: 2,
		},
	}, set)
}
