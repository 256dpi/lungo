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
	client, engine, err := Open(nil, Options{
		Store: NewFileStore("./bench", 0666),
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

	err = os.Remove("./bench")
	if err != nil {
		panic(err)
	}
}
