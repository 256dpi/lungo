package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestCompare(t *testing.T) {
	// equality
	ret := Compare(bson.D{}, bson.D{})
	assert.Equal(t, 0, ret)

	// less than
	ret = Compare("foo", false)
	assert.Equal(t, -1, ret)

	// greater than
	ret = Compare(false, "foo")
	assert.Equal(t, 1, ret)
}
