package lungo

import (
	"math/rand"
	"os"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func BenchmarkMemoryStoreWrite(b *testing.B) {
	client, engine, err := Open(nil, Options{
		Store: NewMemoryStore(),
	})
	if err != nil {
		panic(err)
	}

	defer engine.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err = client.Database("foo").Collection("foo").InsertOne(nil, bson.M{
			"n": i,
		})
		if err != nil {
			panic(err)
		}

		_, err = client.Database("foo").Collection("foo").DeleteMany(nil, bson.M{
			"n": bson.M{
				"$lt": i - 1000,
			},
		})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkMemoryStoreRead(b *testing.B) {
	client, engine, err := Open(nil, Options{
		Store: NewMemoryStore(),
	})
	if err != nil {
		panic(err)
	}

	defer engine.Close()

	for i := 0; i < 100; i++ {
		_, err = client.Database("foo").Collection("foo").InsertOne(nil, bson.M{
			"n": i,
		})
		if err != nil {
			panic(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err = client.Database("foo").Collection("foo").Find(nil, bson.M{
			"n": rand.Intn(100),
		})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSingleFileStoreWrite(b *testing.B) {
	_ = os.Remove("./bench.bson")

	client, engine, err := Open(nil, Options{
		Store: NewFileStore("./bench.bson", 0666),
	})
	if err != nil {
		panic(err)
	}

	defer engine.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err = client.Database("foo").Collection("foo").InsertOne(nil, bson.M{
			"n": i,
		})
		if err != nil {
			panic(err)
		}

		_, err = client.Database("foo").Collection("foo").DeleteMany(nil, bson.M{
			"n": bson.M{
				"$lt": i - 100,
			},
		})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkFileStoreRead(b *testing.B) {
	_ = os.Remove("./bench.bson")

	client, engine, err := Open(nil, Options{
		Store: NewFileStore("./bench.bson", 0666),
	})
	if err != nil {
		panic(err)
	}

	defer engine.Close()

	for i := 0; i < 100; i++ {
		_, err = client.Database("foo").Collection("foo").InsertOne(nil, bson.M{
			"n": i,
		})
		if err != nil {
			panic(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err = client.Database("foo").Collection("foo").Find(nil, bson.M{
			"n": rand.Intn(100),
		})
		if err != nil {
			panic(err)
		}
	}
}
