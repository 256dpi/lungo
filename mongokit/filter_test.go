package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func TestFilter(t *testing.T) {
	a1 := bsonkit.Convert(bson.M{"a": "1", "b": true})
	a2 := bsonkit.Convert(bson.M{"a": "2", "b": false})
	a3 := bsonkit.Convert(bson.M{"a": "3", "b": true})

	// field condition
	list, err := Filter(bsonkit.List{a1, a2, a3}, bsonkit.Convert(bson.M{
		"b": true,
	}), 0)
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{a1, a3}, list)

	// expression
	list, err = Filter(bsonkit.List{a1, a2, a3}, bsonkit.Convert(bson.M{
		"a": bson.M{
			"$gt": "1",
		},
	}), 0)
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{a2, a3}, list)
}
