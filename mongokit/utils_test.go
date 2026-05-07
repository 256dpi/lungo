package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

const testDB = "test-lungo-mongokit"

var testMongoClient *mongo.Client

var testCollCounter = 0

func init() {
	mongoClient, err := mongo.Connect(nil, options.Client().
		ApplyURI("mongodb://localhost").
		SetBSONOptions(&options.BSONOptions{DefaultDocumentM: true}))
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

func toMap(d bsonkit.Doc) bson.M {
	m := make(bson.M, len(*d))
	for _, e := range *d {
		m[e.Key] = e.Value
	}
	return m
}
