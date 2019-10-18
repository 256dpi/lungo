package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestSort(t *testing.T) {
	a1 := Convert(bson.M{"a": "1"})
	a2 := Convert(bson.M{"a": "2"})
	a3 := Convert(bson.M{"a": "3"})

	list := Sort([]bson.D{a3, a1, a2}, "a", false)
	assert.Equal(t, []bson.D{a1, a2, a3}, list)

	list = Sort([]bson.D{a3, a1, a2}, "a", true)
	assert.Equal(t, []bson.D{a3, a2, a1}, list)

	b1 := Convert(bson.M{"b": true})
	b2 := Convert(bson.M{"b": "foo"})
	b3 := Convert(bson.M{"b": 4.2})

	list = Sort([]bson.D{b1, b2, b3}, "b", false)
	assert.Equal(t, []bson.D{b3, b2, b1}, list)

	list = Sort([]bson.D{b1, b2, b3}, "b", true)
	assert.Equal(t, []bson.D{b1, b2, b3}, list)
}
