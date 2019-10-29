package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// TODO: Convert into compatibility test?

func TestSort(t *testing.T) {
	a1 := bsonkit.Convert(bson.M{"a": "1", "b": true})
	a2 := bsonkit.Convert(bson.M{"a": "2", "b": false})
	a3 := bsonkit.Convert(bson.M{"a": "3", "b": true})

	// invalid document
	list, err := Sort(bsonkit.List{a3, a1, a2}, &bson.D{
		bson.E{Key: "a", Value: "0"},
	})
	assert.Error(t, err)
	assert.Nil(t, list)

	// invalid document
	list, err = Sort(bsonkit.List{a3, a1, a2}, &bson.D{
		bson.E{Key: "a", Value: 0},
	})
	assert.Error(t, err)
	assert.Nil(t, list)

	// sort forwards single
	list, err = Sort(bsonkit.List{a3, a1, a2}, &bson.D{
		bson.E{Key: "a", Value: int64(1)},
	})
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{a1, a2, a3}, list)

	// sort backwards single
	list, err = Sort(bsonkit.List{a3, a1, a2}, &bson.D{
		bson.E{Key: "a", Value: int64(-1)},
	})
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{a3, a2, a1}, list)

	// sort forwards multiple
	list, err = Sort(bsonkit.List{a3, a1, a2}, &bson.D{
		bson.E{Key: "b", Value: int64(1)},
		bson.E{Key: "a", Value: int64(1)},
	})
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{a2, a1, a3}, list)

	// sort backwards multiple
	list, err = Sort(bsonkit.List{a3, a1, a2}, &bson.D{
		bson.E{Key: "b", Value: int64(-1)},
		bson.E{Key: "a", Value: int64(-1)},
	})
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{a3, a1, a2}, list)

	// sort mixed
	list, err = Sort(bsonkit.List{a3, a1, a2}, &bson.D{
		bson.E{Key: "b", Value: int64(1)},
		bson.E{Key: "a", Value: int64(-1)},
	})
	assert.NoError(t, err)
	assert.Equal(t, bsonkit.List{a2, a3, a1}, list)
}
