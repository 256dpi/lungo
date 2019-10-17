package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestCompare(t *testing.T) {
	/* equality */

	ret, err := Compare(bson.D{}, bson.D{})
	assert.NoError(t, err)
	assert.Equal(t, 0, ret)

	/* inequality */

	ret, err = Compare("foo", false)
	assert.NoError(t, err)
	assert.Equal(t, -1, ret)
}
