package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestCollectionInsertOne(t *testing.T) {
	collectionTest(t, func(c ICollection) {
		res, err := c.InsertOne(nil, bson.M{})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{},
		}, dumpCollection(c, true))
	})

	collectionTest(t, func(c ICollection) {
		res, err := c.InsertOne(nil, bson.M{
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"foo": "bar",
			},
		}, dumpCollection(c, true))
	})

	collectionTest(t, func(c ICollection) {
		id := primitive.NewObjectID()

		res, err := c.InsertOne(nil, bson.M{
			"_id": id,
			"foo": "bar",
		})
		assert.NoError(t, err)
		assert.True(t, !res.InsertedID.(primitive.ObjectID).IsZero())
		assert.Equal(t, []bson.M{
			{
				"_id": id,
				"foo": "bar",
			},
		}, dumpCollection(c, false))
	})
}
