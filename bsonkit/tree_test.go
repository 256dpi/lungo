package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestIndex(t *testing.T) {
	d1 := Convert(bson.M{"a": "1"})
	d2 := Convert(bson.M{"a": "1"})

	tree := NewTree(false, []Column{
		{Path: "a"},
	})
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))

	ok := tree.Add(d1)
	assert.True(t, ok)
	assert.True(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))

	ok = tree.Add(d1)
	assert.False(t, ok)
	assert.True(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))

	ok = tree.Add(d2)
	assert.True(t, ok)
	assert.True(t, tree.Has(d1))
	assert.True(t, tree.Has(d2))

	ok = tree.Remove(d1)
	assert.True(t, ok)
	assert.False(t, tree.Has(d1))
	assert.True(t, tree.Has(d2))

	ok = tree.Remove(d2)
	assert.True(t, ok)
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))

	ok = tree.Remove(d2)
	assert.False(t, ok)
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
}

func TestCompoundIndex(t *testing.T) {
	d1 := Convert(bson.M{"a": "1", "b": true})
	d2 := Convert(bson.M{"a": "1", "b": false})

	tree := NewTree(false, []Column{
		{Path: "a"},
		{Path: "b"},
	})
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))

	ok := tree.Add(d1)
	assert.True(t, ok)
	assert.True(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))

	ok = tree.Add(d1)
	assert.False(t, ok)
	assert.True(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))

	ok = tree.Add(d2)
	assert.True(t, ok)
	assert.True(t, tree.Has(d1))
	assert.True(t, tree.Has(d2))

	ok = tree.Remove(d1)
	assert.True(t, ok)
	assert.False(t, tree.Has(d1))
	assert.True(t, tree.Has(d2))

	ok = tree.Remove(d2)
	assert.True(t, ok)
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))

	ok = tree.Remove(d2)
	assert.False(t, ok)
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
}

func TestUniqueIndex(t *testing.T) {
	d1 := Convert(bson.M{"a": "1"})
	d2 := Convert(bson.M{"a": "2"})
	d3 := Convert(bson.M{"a": "2"})

	tree := NewTree(true, []Column{
		{Path: "a"},
	})
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))

	ok := tree.Add(d1)
	assert.True(t, ok)
	assert.True(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))

	ok = tree.Add(d1)
	assert.False(t, ok)
	assert.True(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))

	ok = tree.Add(d2)
	assert.True(t, ok)
	assert.True(t, tree.Has(d1))
	assert.True(t, tree.Has(d2))
	assert.True(t, tree.Has(d3))

	ok = tree.Remove(d1)
	assert.True(t, ok)
	assert.False(t, tree.Has(d1))
	assert.True(t, tree.Has(d2))
	assert.True(t, tree.Has(d3))

	ok = tree.Remove(d2)
	assert.True(t, ok)
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))

	ok = tree.Remove(d2)
	assert.False(t, ok)
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))
}

func TestUniqueCompoundIndex(t *testing.T) {
	d1 := Convert(bson.M{"a": "1", "b": true})
	d2 := Convert(bson.M{"a": "2", "b": true})
	d3 := Convert(bson.M{"a": "2", "b": true})

	tree := NewTree(true, []Column{
		{Path: "a"},
		{Path: "b"},
	})
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))

	ok := tree.Add(d1)
	assert.True(t, ok)
	assert.True(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))

	ok = tree.Add(d1)
	assert.False(t, ok)
	assert.True(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))

	ok = tree.Add(d2)
	assert.True(t, ok)
	assert.True(t, tree.Has(d1))
	assert.True(t, tree.Has(d2))
	assert.True(t, tree.Has(d3))

	ok = tree.Remove(d1)
	assert.True(t, ok)
	assert.False(t, tree.Has(d1))
	assert.True(t, tree.Has(d2))
	assert.True(t, tree.Has(d3))

	ok = tree.Remove(d2)
	assert.True(t, ok)
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))

	ok = tree.Remove(d2)
	assert.False(t, ok)
	assert.False(t, tree.Has(d1))
	assert.False(t, tree.Has(d2))
	assert.False(t, tree.Has(d3))
}

func TestIndexClone(t *testing.T) {
	d1 := Convert(bson.M{"a": "1"})
	d2 := Convert(bson.M{"a": "2"})
	d3 := Convert(bson.M{"a": "2"})

	tree1 := NewTree(false, []Column{
		{Path: "a"},
	})

	tree1.Add(d1)
	tree1.Add(d2)

	tree2 := tree1.Clone()
	tree2.Add(d3)
	tree2.Remove(d1)

	assert.True(t, tree1.Has(d1))
	assert.True(t, tree1.Has(d2))
	assert.False(t, tree1.Has(d3))

	assert.False(t, tree2.Has(d1))
	assert.True(t, tree2.Has(d2))
	assert.True(t, tree2.Has(d3))
}
