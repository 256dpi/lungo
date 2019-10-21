package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func TestIndex(t *testing.T) {
	d1 := bsonkit.Convert(bson.M{"a": "1"})
	d2 := bsonkit.Convert(bson.M{"a": "2"})
	d3 := bsonkit.Convert(bson.M{"a": "2"})

	index := NewIndex(false, []bsonkit.Column{
		{Path: "a"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set := index.Add(d1)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set = index.Add(d1)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set = index.Add(d2)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	index.Remove(d1)
	assert.True(t, set)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	index.Remove(d2)
	assert.True(t, set)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
}

func TestCompoundIndex(t *testing.T) {
	d1 := bsonkit.Convert(bson.M{"a": "1", "b": true})
	d2 := bsonkit.Convert(bson.M{"a": "2", "b": true})
	d3 := bsonkit.Convert(bson.M{"a": "2", "b": true})

	index := NewIndex(false, []bsonkit.Column{
		{Path: "a"},
		{Path: "b"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set := index.Add(d1)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set = index.Add(d1)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set = index.Add(d2)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	index.Remove(d1)
	assert.True(t, set)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	index.Remove(d2)
	assert.True(t, set)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
}

func TestUniqueIndex(t *testing.T) {
	d1 := bsonkit.Convert(bson.M{"a": "1"})
	d2 := bsonkit.Convert(bson.M{"a": "2"})
	d3 := bsonkit.Convert(bson.M{"a": "2"})

	index := NewIndex(true, []bsonkit.Column{
		{Path: "a"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set := index.Add(d1)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set = index.Add(d1)
	assert.False(t, set)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set = index.Add(d2)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))

	index.Remove(d1)
	assert.True(t, set)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))

	index.Remove(d2)
	assert.True(t, set)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
}

func TestUniqueCompoundIndex(t *testing.T) {
	d1 := bsonkit.Convert(bson.M{"a": "1", "b": true})
	d2 := bsonkit.Convert(bson.M{"a": "2", "b": true})
	d3 := bsonkit.Convert(bson.M{"a": "2", "b": true})

	index := NewIndex(true, []bsonkit.Column{
		{Path: "a"},
		{Path: "b"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set := index.Add(d1)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set = index.Add(d1)
	assert.False(t, set)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))

	set = index.Add(d2)
	assert.True(t, set)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))

	index.Remove(d1)
	assert.True(t, set)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))

	index.Remove(d2)
	assert.True(t, set)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
}

func TestIndexClone(t *testing.T) {
	d1 := bsonkit.Convert(bson.M{"a": "1"})
	d2 := bsonkit.Convert(bson.M{"a": "2"})
	d3 := bsonkit.Convert(bson.M{"a": "2"})

	index1 := NewIndex(false, []bsonkit.Column{
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
