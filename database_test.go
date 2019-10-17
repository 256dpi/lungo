package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func TestDatabaseClient(t *testing.T) {
	clientTest(t, func(t *testing.T, c IClient) {
		assert.Equal(t, c, c.Database("").Client())
	})
}

func TestDatabaseListCollectionsAndNames(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		names, err := d.ListCollectionNames(nil, bson.M{
			"name": bson.M{"$in": bson.A{"coll-names"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{}, names)

		csr, err := d.ListCollections(nil, bson.M{
			"name": bson.M{"$in": bson.A{"coll-names"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{}, readAll(csr))

		_, err = d.Collection("coll-names").InsertOne(nil, bson.M{
			"name": bson.M{"$in": bson.A{"coll-names"}},
		})
		assert.NoError(t, err)

		names, err = d.ListCollectionNames(nil, bson.M{
			"name": bson.M{"$in": bson.A{"coll-names"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{"coll-names"}, names)

		csr, err = d.ListCollections(nil, bson.M{
			"name": bson.M{"$in": bson.A{"coll-names"}},
		})
		assert.NoError(t, err)

		res := readAll(csr)
		assert.Len(t, res, 1)
		assert.Equal(t, "coll-names", res[0]["name"])
		assert.Equal(t, "collection", res[0]["type"])
		assert.Equal(t, bson.M{}, res[0]["options"])
		assert.Equal(t, false, res[0]["info"].(bson.M)["readOnly"])
	})
}

func TestDatabaseName(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Equal(t, testDB, d.Name())
	})
}

func TestDatabaseReadConcern(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Equal(t, readconcern.New(), d.ReadConcern())
	})
}

func TestDatabaseReadPreference(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Equal(t, readpref.Primary(), d.ReadPreference())
	})
}

func TestDatabaseWriteConcern(t *testing.T) {
	databaseTest(t, func(t *testing.T, d IDatabase) {
		assert.Nil(t, d.WriteConcern())
	})
}
