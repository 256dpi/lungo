package mongokit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/SaveliiYam/lungo/bsonkit"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const testDB = "test-lungo-mongokit"

var testMongoClient *mongo.Client

var testCollCounter = 0

func init() {
	mongoClient, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err == nil {
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = mongoClient.Ping(pingCtx, readpref.Primary())
		cancel()
		if err == nil {
			_ = mongoClient.Database(testDB).Drop(nil)
			testMongoClient = mongoClient
		}
	}
}

func skipUnlessMongo(t *testing.T) {
	if testMongoClient == nil {
		t.Skip("mongodb not available")
	}
}

func testCollection() *mongo.Collection {
	testCollCounter++
	name := fmt.Sprintf("n-%d", testCollCounter)
	return testMongoClient.Database(testDB).Collection(name)
}

// assertBSONEqual compares documents ignoring bson.D vs bson.M and key order in nested docs.
func assertBSONEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	t.Helper()
	assert.Equal(t, normalizeBSONDoc(expected), normalizeBSONDoc(actual), msgAndArgs...)
}

func normalizeBSONDoc(v interface{}) interface{} {
	switch d := v.(type) {
	case bson.D:
		return bsonkit.ToMapValue(d)
	case *bson.D:
		if d == nil {
			return nil
		}
		return bsonkit.ToMap(d)
	case bson.M:
		return bsonkit.ToMapValue(d)
	default:
		return bsonkit.ToMapValue(v)
	}
}

func convertArray(array []interface{}) bson.A {
	a := make(bson.A, 0, len(array))
	for _, item := range array {
		if arr, ok := item.([]interface{}); ok {
			item = convertArray(arr)
		}

		a = append(a, item)
	}
	return a
}
