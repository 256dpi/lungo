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

func TestPick(t *testing.T) {
	a1 := Convert(bson.M{"a": "1"})
	a2 := Convert(bson.M{"a": "2"})
	a3 := Convert(bson.M{"a": "2"})
	b1 := Convert(bson.M{"b": "3"})

	// raw values
	res := Pick(List{a1, a2, b1, a3}, "a", false)
	assert.Equal(t, bson.A{"1", "2", Missing, "2"}, res)

	// compact values
	res = Pick(List{a1, b1, a2, a3}, "a", true)
	assert.Equal(t, bson.A{"1", "2", "2"}, res)
}

func TestCollect(t *testing.T) {
	a1 := Convert(bson.M{"a": "1"})
	a2 := Convert(bson.M{"a": "2"})
	a3 := Convert(bson.M{"a": "2"})
	b1 := Convert(bson.M{"b": "3"})

	// raw values
	res := Collect(List{a1, a2, b1, a3}, "a", false, false, false, false)
	assert.Equal(t, bson.A{"1", "2", Missing, "2"}, res)

	// compact values
	res = Collect(List{a1, b1, a2, a3}, "a", true, false, false, false)
	assert.Equal(t, bson.A{"1", "2", "2"}, res)

	// distinct values
	res = Collect(List{a1, b1, a2, a3}, "a", false, false, false, true)
	assert.Equal(t, bson.A{Missing, "1", "2"}, res)

	// compact and distinct values
	res = Collect(List{a1, b1, a2, a1, a3, a1}, "a", true, false, false, true)
	assert.Equal(t, bson.A{"1", "2"}, res)
}

func TestCollectArray(t *testing.T) {
	a1 := Convert(bson.M{"a": "1"})
	a2 := Convert(bson.M{"a": bson.A{"1", "2"}})
	a3 := Convert(bson.M{"a": "2"})

	// raw values
	res := Collect(List{a1, a2, a3}, "a", false, false, false, false)
	assert.Equal(t, bson.A{"1", bson.A{"1", "2"}, "2"}, res)

	// flattened values
	res = Collect(List{a1, a2, a3}, "a", false, false, true, false)
	assert.Equal(t, bson.A{"1", "1", "2", "2"}, res)

	// distinct flattened values
	res = Collect(List{a1, a2, a3}, "a", false, false, true, true)
	assert.Equal(t, bson.A{"1", "2"}, res)
}

func TestCollectEmbedded(t *testing.T) {
	a1 := Convert(bson.M{"a": bson.A{bson.M{"b": "1"}}})
	a2 := Convert(bson.M{"a": bson.A{bson.M{"b": bson.A{"1", "2"}}}})
	a3 := Convert(bson.M{"a": bson.A{bson.M{"b": "1"}, bson.M{"b": "2"}}})

	// raw values
	res := Collect(List{a1, a2, a3}, "a.b", false, false, false, false)
	assert.Equal(t, bson.A{bson.A{"1"}, bson.A{bson.A{"1", "2"}}, bson.A{"1", "2"}}, res)

	// merged values
	res = Collect(List{a1, a2, a3}, "a.b", false, true, false, false)
	assert.Equal(t, bson.A{bson.A{"1"}, bson.A{"1", "2"}, bson.A{"1", "2"}}, res)

	// flattened values
	res = Collect(List{a1, a2, a3}, "a.b", false, true, true, false)
	assert.Equal(t, bson.A{"1", "1", "2", "1", "2"}, res)

	// distinct values
	res = Collect(List{a1, a2, a3}, "a.b", false, true, true, true)
	assert.Equal(t, bson.A{"1", "2"}, res)
}
