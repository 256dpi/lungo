package lungo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func TestDatabaseClient(t *testing.T) {
	clientTest(t, func(t *testing.T, c IClient) {
		assert.Equal(t, c, c.Database("").Client())
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
