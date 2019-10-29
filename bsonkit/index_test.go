package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestIndex(t *testing.T) {
	d1 := Convert(bson.M{"a": "1"})
	d2 := Convert(bson.M{"a": "1"})

	index := NewIndex(false, []Column{
		{Path: "a"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))

	ok := index.Add(d1)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))

	ok = index.Add(d1)
	assert.False(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))

	ok = index.Add(d2)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))

	ok = index.Remove(d2)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))

	ok = index.Remove(d2)
	assert.False(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
}

func TestIndexCompound(t *testing.T) {
	d1 := Convert(bson.M{"a": "1", "b": true})
	d2 := Convert(bson.M{"a": "1", "b": false})

	index := NewIndex(false, []Column{
		{Path: "a"},
		{Path: "b"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))

	ok := index.Add(d1)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))

	ok = index.Add(d1)
	assert.False(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))

	ok = index.Add(d2)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))

	ok = index.Remove(d2)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))

	ok = index.Remove(d2)
	assert.False(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
}

func TestIndexUnique(t *testing.T) {
	d1 := Convert(bson.M{"a": "1"})
	d2 := Convert(bson.M{"a": "2"})
	d3 := Convert(bson.M{"a": "2"})

	index := NewIndex(true, []Column{
		{Path: "a"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	ok := index.Add(d1)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	ok = index.Add(d1)
	assert.False(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	ok = index.Add(d2)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))

	ok = index.Remove(d2)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	ok = index.Remove(d2)
	assert.False(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
}

func TestIndexCompoundUnique(t *testing.T) {
	d1 := Convert(bson.M{"a": "1", "b": true})
	d2 := Convert(bson.M{"a": "2", "b": true})
	d3 := Convert(bson.M{"a": "2", "b": true})

	index := NewIndex(true, []Column{
		{Path: "a"},
		{Path: "b"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	ok := index.Add(d1)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	ok = index.Add(d1)
	assert.False(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	ok = index.Add(d2)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))

	ok = index.Remove(d2)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	ok = index.Remove(d2)
	assert.False(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
}

func TestIndexClone(t *testing.T) {
	d1 := Convert(bson.M{"a": "1"})
	d2 := Convert(bson.M{"a": "2"})
	d3 := Convert(bson.M{"a": "2"})

	index1 := NewIndex(false, []Column{
		{Path: "a"},
	})

	index1.Add(d1)
	index1.Add(d2)

	index2 := index1.Clone()
	index2.Add(d3)
	index2.Remove(d1)

	assert.True(t, index1.Has(d1))
	assert.True(t, index1.Has(d2))
	assert.False(t, index1.Has(d3))

	assert.False(t, index2.Has(d1))
	assert.True(t, index2.Has(d2))
	assert.True(t, index2.Has(d3))
}
