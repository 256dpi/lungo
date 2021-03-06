package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestCompare(t *testing.T) {
	// equality
	assert.Equal(t, 0, Compare(bson.D{}, bson.D{}))

	// less than
	assert.Equal(t, -1, Compare("foo", false))

	// greater than
	assert.Equal(t, 1, Compare(false, "foo"))

	// decimal
	dec, err := primitive.ParseDecimal128("3.14")
	assert.NoError(t, err)
	assert.Equal(t, 1, Compare(5.0, dec))
}
