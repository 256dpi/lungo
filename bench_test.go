package lungo

import (
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func BenchmarkMemoryStore(b *testing.B) {
	client, err := Open(nil, AltClientOptions{
		Store: NewMemoryStore(),
	})
	if err != nil {
		panic(err)
	}

	doc := bson.M{"foo": "bar"}

	for i := 0; i < b.N; i++ {
		_, err = client.Database("foo").Collection("foo").InsertOne(nil, doc)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSingleFileStore(b *testing.B) {
	client, err := Open(nil, AltClientOptions{
		Store: NewSingleFileStore("./bench", 0666),
	})
	if err != nil {
		panic(err)
	}

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
