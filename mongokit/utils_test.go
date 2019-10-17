package mongokit

import (
	"fmt"

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
