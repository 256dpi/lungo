package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestIndex(t *testing.T) {
	d1 := MustConvert(bson.M{"a": "1"})
	d2 := MustConvert(bson.M{"a": "1"})

	index := NewIndex(false, []Column{
		{Path: "a"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{}, index.List())

	ok := index.Add(d1)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{d1}, index.List())

	ok = index.Add(d1)
	assert.False(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{d1}, index.List())

	ok = index.Add(d2)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.Equal(t, List{d1, d2}, index.List())

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.Equal(t, List{d2}, index.List())

	ok = index.Remove(d2)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{}, index.List())

	ok = index.Remove(d2)
	assert.False(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{}, index.List())
}

func TestIndexCompound(t *testing.T) {
	d1 := MustConvert(bson.M{"a": "1", "b": true})
	d2 := MustConvert(bson.M{"a": "1", "b": false})

	index := NewIndex(false, []Column{
		{Path: "a"},
		{Path: "b"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{}, index.List())

	ok := index.Add(d1)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{d1}, index.List())

	ok = index.Add(d1)
	assert.False(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{d1}, index.List())

	ok = index.Add(d2)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.Equal(t, List{d2, d1}, index.List())

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.Equal(t, List{d2}, index.List())

	ok = index.Remove(d2)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{}, index.List())

	ok = index.Remove(d2)
	assert.False(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.Equal(t, List{}, index.List())
}

func TestIndexUnique(t *testing.T) {
	d1 := MustConvert(bson.M{"a": "1"})
	d2 := MustConvert(bson.M{"a": "2"})
	d3 := MustConvert(bson.M{"a": "2"})

	index := NewIndex(true, []Column{
		{Path: "a"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{}, index.List())

	ok := index.Add(d1)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{d1}, index.List())

	ok = index.Add(d1)
	assert.False(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{d1}, index.List())

	ok = index.Add(d2)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))
	assert.Equal(t, List{d1, d2}, index.List())

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))
	assert.Equal(t, List{d2}, index.List())

	ok = index.Remove(d2)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{}, index.List())

	ok = index.Remove(d2)
	assert.False(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{}, index.List())
}

func TestIndexCompoundUnique(t *testing.T) {
	d1 := MustConvert(bson.M{"a": "1", "b": true})
	d2 := MustConvert(bson.M{"a": "2", "b": true})
	d3 := MustConvert(bson.M{"a": "2", "b": true})

	index := NewIndex(true, []Column{
		{Path: "a"},
		{Path: "b"},
	})
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{}, index.List())

	ok := index.Add(d1)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{d1}, index.List())

	ok = index.Add(d1)
	assert.False(t, ok)
	assert.True(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{d1}, index.List())

	ok = index.Add(d2)
	assert.True(t, ok)
	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))
	assert.Equal(t, List{d1, d2}, index.List())

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.True(t, index.Has(d3))
	assert.Equal(t, List{d2}, index.List())

	ok = index.Remove(d2)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{}, index.List())

	ok = index.Remove(d2)
	assert.False(t, ok)
	assert.False(t, index.Has(d1))
	assert.False(t, index.Has(d2))
	assert.False(t, index.Has(d3))
	assert.Equal(t, List{}, index.List())
}

func TestIndexClone(t *testing.T) {
	d1 := MustConvert(bson.M{"a": "1"})
	d2 := MustConvert(bson.M{"a": "2"})
	d3 := MustConvert(bson.M{"a": "2"})

	index1 := NewIndex(false, []Column{
		{Path: "a"},
	})

	ok := index1.Add(d1)
	assert.True(t, ok)

	ok = index1.Add(d2)
	assert.True(t, ok)

	index2 := index1.Clone()

	ok = index2.Add(d3)
	assert.True(t, ok)

	ok = index2.Remove(d1)
	assert.True(t, ok)

	assert.True(t, index1.Has(d1))
	assert.True(t, index1.Has(d2))
	assert.False(t, index1.Has(d3))
	assert.Equal(t, List{d1, d2}, index1.List())

	assert.False(t, index2.Has(d1))
	assert.True(t, index2.Has(d2))
	assert.True(t, index2.Has(d3))
	assert.Equal(t, List{d2, d3}, index2.List())
}

func TestIndexMultiKey(t *testing.T) {
	d1 := MustConvert(bson.M{"tags": bson.A{"x", "y"}})
	d2 := MustConvert(bson.M{"tags": bson.A{"y", "z"}})

	index := NewIndex(false, []Column{
		{Path: "tags"},
	})

	ok := index.Add(d1)
	assert.True(t, ok)
	ok = index.Add(d2)
	assert.True(t, ok)

	assert.True(t, index.Has(d1))
	assert.True(t, index.Has(d2))

	// list returns each document once even though d1 and d2 share key "y"
	list := index.List()
	assert.Len(t, list, 2)

	ok = index.Remove(d1)
	assert.True(t, ok)
	assert.False(t, index.Has(d1))
	assert.True(t, index.Has(d2))
	assert.Equal(t, List{d2}, index.List())
}

func TestIndexMultiKeyNested(t *testing.T) {
	// path traverses an array of embedded docs — MongoDB multikey expands
	// each pet's name into a separate index entry
	d1 := MustConvert(bson.M{"pets": bson.A{
		bson.M{"name": "fido"},
		bson.M{"name": "rex"},
	}})
	d2 := MustConvert(bson.M{"pets": bson.A{
		bson.M{"name": "rex"},
	}})

	index := NewIndex(true, []Column{
		{Path: "pets.name"},
	})

	ok := index.Add(d1)
	assert.True(t, ok)

	// d2's "rex" collides with d1's "rex" — must be rejected
	ok = index.Add(d2)
	assert.False(t, ok)

	// remove d1 frees both names
	ok = index.Remove(d1)
	assert.True(t, ok)

	ok = index.Add(d2)
	assert.True(t, ok)
}

func TestIndexMultiKeyNestedFlatten(t *testing.T) {
	// path traverses an array of embedded docs whose field is itself an
	// array — MongoDB flattens to a single set of keys
	d1 := MustConvert(bson.M{"pets": bson.A{
		bson.M{"tags": bson.A{"a", "b"}},
		bson.M{"tags": bson.A{"c"}},
	}})
	d2 := MustConvert(bson.M{"pets": bson.A{
		bson.M{"tags": bson.A{"b"}},
	}})

	index := NewIndex(true, []Column{
		{Path: "pets.tags"},
	})

	ok := index.Add(d1)
	assert.True(t, ok)

	// d2's "b" collides with d1's flattened "b" — must be rejected
	ok = index.Add(d2)
	assert.False(t, ok)
}

func TestIndexEmptyArrayDistinctFromMissing(t *testing.T) {
	// {tags: []} and {} must occupy distinct unique-index slots — MongoDB
	// indexes empty arrays under the array sentinel, not under null/missing
	d1 := MustConvert(bson.M{"tags": bson.A{}})
	d2 := MustConvert(bson.M{})

	index := NewIndex(true, []Column{
		{Path: "tags"},
	})

	ok := index.Add(d1)
	assert.True(t, ok)

	ok = index.Add(d2)
	assert.True(t, ok)

	// a second empty-array doc still collides with d1
	d3 := MustConvert(bson.M{"tags": bson.A{}})
	ok = index.Add(d3)
	assert.False(t, ok)
}

func TestIndexMultiKeyUnique(t *testing.T) {
	d1 := MustConvert(bson.M{"tags": bson.A{"x"}})
	d2 := MustConvert(bson.M{"tags": bson.A{"x", "y"}})
	d3 := MustConvert(bson.M{"tags": bson.A{"y", "x"}})
	d4 := MustConvert(bson.M{"tags": bson.A{"z"}})

	index := NewIndex(true, []Column{
		{Path: "tags"},
	})

	ok := index.Add(d1)
	assert.True(t, ok)

	// d2 overlaps on "x" with d1 — must be rejected
	ok = index.Add(d2)
	assert.False(t, ok)

	// d3 overlaps on "x" with d1 — order doesn't matter
	ok = index.Add(d3)
	assert.False(t, ok)

	// d4 has no overlap — admitted
	ok = index.Add(d4)
	assert.True(t, ok)

	// remove d1, then d2 fits
	ok = index.Remove(d1)
	assert.True(t, ok)

	ok = index.Add(d2)
	assert.True(t, ok)
}
