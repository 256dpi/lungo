package lungo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestIsUniquenessError(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.Indexes().CreateOne(context.Background(), mongo.IndexModel{
			Keys:    bson.M{"title": 1},
			Options: options.Index().SetUnique(true),
		})
		assert.NoError(t, err)

		_, err = c.InsertOne(nil, bson.M{
			"title": "foo",
		})
		assert.NoError(t, err)

		_, err = c.InsertOne(nil, bson.M{
			"title": "foo",
		})
		assert.Error(t, err)
		assert.True(t, IsUniquenessError(err))
	})
}
