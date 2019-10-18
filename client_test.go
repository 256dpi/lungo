package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestClientListDatabasesAndNames(t *testing.T) {
	clientTest(t, func(t *testing.T, c IClient) {
		err := c.Database(testDB).Drop(nil)
		assert.NoError(t, err)

		names, err := c.ListDatabaseNames(nil, bson.M{
			"name": testDB,
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{}, names)

		res, err := c.ListDatabases(nil, bson.M{
			"name": testDB,
		})
		assert.NoError(t, err)
		assert.Equal(t, mongo.ListDatabasesResult{
			Databases: make([]mongo.DatabaseSpecification, 0),
		}, res)

		_, err = c.Database(testDB).Collection("foo").InsertOne(nil, bson.M{
			"name": testDB,
		})
		assert.NoError(t, err)

		names, err = c.ListDatabaseNames(nil, bson.M{
			"name": testDB,
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{testDB}, names)

		res, err = c.ListDatabases(nil, bson.M{
			"name": testDB,
		})
		assert.NoError(t, err)
		assert.Equal(t, mongo.ListDatabasesResult{
			Databases: []mongo.DatabaseSpecification{
				{
					Name:       testDB,
					SizeOnDisk: res.Databases[0].SizeOnDisk,
					Empty:      false,
				},
			},
			TotalSize: res.TotalSize,
		}, res)
	})
}
