package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const testDB = "test-lungo-mongokit"

var testMongoClient *mongo.Client

var testCollCounter = 0

func init() {
	mongoClient, err := mongo.Connect(nil, options.Client().ApplyURI("mongodb://localhost"))
	if err != nil {
		panic(err)
	}

	err = mongoClient.Database(testDB).Drop(nil)
	if err != nil {
		panic(err)
	}

	testMongoClient = mongoClient
}

func testCollection() *mongo.Collection {
	testCollCounter++
	name := fmt.Sprintf("n-%d", testCollCounter)
	return testMongoClient.Database(testDB).Collection(name)
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
