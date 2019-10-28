package lungo

import (
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func BenchmarkMemoryStore(b *testing.B) {
	client, engine, err := Open(nil, Options{
		Store: NewMemoryStore(),
	})
	if err != nil {
		panic(err)
	}

	defer engine.Close()

	doc := bson.M{"foo": "bar"}

	for i := 0; i < b.N; i++ {
		_, err = client.Database("foo").Collection("foo").InsertOne(nil, doc)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSingleFileStore(b *testing.B) {
	_ = os.Remove("./bench.bson")

	client, engine, err := Open(nil, Options{
		Store: NewFileStore("./bench.bson", 0666),
	})
	if err != nil {
		panic(err)
	}

	defer engine.Close()

	doc := bson.M{"foo": "bar"}

	for i := 0; i < b.N; i++ {
		_, err = client.Database("foo").Collection("foo").InsertOne(nil, doc)
		if err != nil {
			panic(err)
		}
	}
}
