package lungo

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const testDB = "test-lungo"

var testMongoClient IClient
var testLungoClient IClient

var testCollCounter = 0

func init() {
	mongoClient, err := Connect(nil, options.Client().ApplyURI("mongodb://localhost"))
	if err != nil {
		panic(err)
	}

	err = mongoClient.Database(testDB).Drop(nil)
	if err != nil {
		panic(err)
	}

	testMongoClient = mongoClient

	lungoClient, _, err := Open(nil, Options{
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

func collectionTest(t *testing.T, fn func(t *testing.T, c ICollection)) {
	testCollCounter++
	name := fmt.Sprintf("n-%d", testCollCounter)

	clientTest(t, func(t *testing.T, client IClient) {
		fn(t, client.Database(testDB).Collection(name))
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
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(ms * time.Millisecond)
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
			f = strings.ReplaceAll(f, a, b)
		}

		// add method
		list = append(list, m.Name+f)
	}

	return list
}
