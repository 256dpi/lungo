package lungo

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestOpenGoroutineLeak(t *testing.T) {
	num := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		_, engine, err := Open(nil, Options{
			Store: NewMemoryStore(),
		})
		assert.NoError(t, err)
		engine.Close()
	}

	// a leak means the goroutine count grows past the baseline; tolerate
	// transient drops or jitter from unrelated runtime/test goroutines by
	// polling briefly until the count settles at or below baseline
	deadline := time.Now().Add(time.Second)
	current := runtime.NumGoroutine()
	for current > num && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		current = runtime.NumGoroutine()
	}
	assert.LessOrEqual(t, current, num)
}

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
