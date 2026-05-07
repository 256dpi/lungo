package lungo

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const testDB = "test-lungo"

var testMongoClient IClient
var testLungoClient IClient
var testLungoEngine *Engine

var testCollCounter = 0

func init() {
	mongoClient, err := Connect(nil, options.Client().
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

	lungoClient, lungoEngine, err := Open(nil, Options{
		Store:          NewMemoryStore(),
		ExpireInterval: 100 * time.Millisecond,
		ExpireErrors: func(err error) {
			panic(err)
		},
	})
	if err != nil {
		panic(err)
	}

	testLungoClient = lungoClient
	testLungoEngine = lungoEngine
}

func clientTest(t *testing.T, fn func(t *testing.T, c IClient)) {
	t.Run("Mongo", func(t *testing.T) {
		fn(t, testMongoClient)
	})

	t.Run("Lungo", func(t *testing.T) {
		fn(t, testLungoClient)
	})
}

func databaseTest(t *testing.T, fn func(t *testing.T, d IDatabase)) {
	clientTest(t, func(t *testing.T, client IClient) {
		fn(t, client.Database(testDB))
	})
}

func collectionName() string {
	testCollCounter++
	return fmt.Sprintf("n-%d", testCollCounter)
}

func collectionTest(t *testing.T, fn func(t *testing.T, c ICollection)) {
	clientTest(t, func(t *testing.T, client IClient) {
		fn(t, client.Database(testDB).Collection(collectionName()))
	})
}

func bucketTest(t *testing.T, chunkSize int32, fn func(t *testing.T, b *Bucket)) {
	if chunkSize == 0 {
		chunkSize = mongo.DefaultGridFSChunkSize
	}
	clientTest(t, func(t *testing.T, client IClient) {
		fn(t, NewBucket(client.Database(testDB), options.GridFSBucket().SetName(collectionName()).SetChunkSizeBytes(chunkSize)))
	})
}

func gridfsTest(t *testing.T, fn func(t *testing.T, b *mongo.GridFSBucket)) {
	db := testMongoClient.Database(testDB).(*MongoDatabase).Database
	name := collectionName()
	b := db.GridFSBucket(options.GridFSBucket().SetName(name))

	t.Run("GridFS", func(t *testing.T) {
		fn(t, b)
	})
}

func readAll(csr ICursor) []bson.M {
	if csr == nil {
		return nil
	}

	out := make([]bson.M, 0)
	err := csr.All(nil, &out)
	if err != nil {
		panic(err)
	}

	return out
}

func dumpCollection(c ICollection, clean bool) []bson.M {
	csr, err := c.Find(nil, bson.M{})
	if err != nil {
		panic(err)
	}

	out := readAll(csr)

	if clean {
		for _, item := range out {
			delete(item, "_id")
		}
	}

	return out
}

func timeout(ms time.Duration) context.Context {
	// return cancelled context if negative
	if ms <= 0 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}

	// return context with deadline
	ctx, cancel := context.WithTimeout(context.Background(), ms*time.Millisecond)
	go func() {
		time.Sleep(10 * ms * time.Millisecond)
		cancel()
	}()

	return ctx
}

func methods(t reflect.Type, replacements map[string]string, skip ...string) []string {
	// prepare list
	var list []string

	// handle all methods
	for i := 0; i < t.NumMethod(); i++ {
		// get method
		m := t.Method(i)

		// skip lowercase methods
		if unicode.IsLower(rune(m.Name[0])) {
			continue
		}

		// check if skipped
		var skipped bool
		for _, name := range skip {
			if name == m.Name {
				skipped = true
			}
		}
		if skipped {
			continue
		}

		// get signature
		f := m.Type.String()[4:]

		// remove first argument if not interface
		if t.Kind() != reflect.Interface {
			c := strings.Index(f, ",")
			if c >= 0 && c < strings.Index(f, ")") {
				f = "(" + f[c+2:]
			} else {
				c = strings.Index(f, ")")
				f = "(" + f[c:]
			}
		}

		// replace types
		for a, b := range replacements {
			r := regexp.MustCompile("([\\(\\s])" + regexp.QuoteMeta(a) + "([\\,\\)]|$)")
			f = r.ReplaceAllString(f, "${1}"+b+"${2}")
		}

		// add method
		list = append(list, m.Name+f)
	}

	return list
}

func runWithin(t *testing.T, d time.Duration, msg string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal(msg)
	}
}
