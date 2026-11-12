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

	"github.com/SaveliiYam/lungo/bsonkit"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const testDB = "test-lungo"

var testMongoClient IClient
var testLungoClient IClient
var testLungoEngine *Engine

var testCollCounter = 0

func init() {
	clientOpt := options.Client().ApplyURI("mongodb://localhost:27017")
	mongoClient, err := Connect(nil, clientOpt)
	if err == nil {
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = mongoClient.Ping(pingCtx, readpref.Primary())
		cancel()
		if err == nil {
			_ = mongoClient.Database(testDB).Drop(nil)
			testMongoClient = mongoClient
		}
	}

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
		if testMongoClient == nil {
			t.Skip("mongodb not available")
		}
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
		chunkSize = options.DefaultChunkSize
	}
	clientTest(t, func(t *testing.T, client IClient) {
		fn(t, NewBucket(client.Database(testDB), mat(options.GridFSBucket().SetName(collectionName()).SetChunkSizeBytes(chunkSize))))
	})
}

func gridfsTest(t *testing.T, fn func(t *testing.T, b *mongo.GridFSBucket)) {
	if testMongoClient == nil {
		t.Skip("mongodb not available")
	}
	mc := testMongoClient.(*MongoClient)
	name := collectionName()
	b := mc.client.Database(testDB).GridFSBucket(options.GridFSBucket().SetName(name))

	t.Run("GridFS", func(t *testing.T) {
		fn(t, b)
	})
}

func mat[T any](l options.Lister[T]) *T {
	o, _ := materialize(l)
	return o
}

func matCount(b *options.CountOptionsBuilder) *options.CountOptions { return mat(b) }
func matFind(b *options.FindOptionsBuilder) *options.FindOptions    { return mat(b) }

func withoutAckBulk(r *mongo.BulkWriteResult) *mongo.BulkWriteResult {
	if r == nil {
		return nil
	}
	c := *r
	c.Acknowledged = false
	return &c
}

func withoutAckDelete(r *mongo.DeleteResult) *mongo.DeleteResult {
	if r == nil {
		return nil
	}
	c := *r
	c.Acknowledged = false
	return &c
}

func withoutAckUpdate(r *mongo.UpdateResult) *mongo.UpdateResult {
	if r == nil {
		return nil
	}
	c := *r
	c.Acknowledged = false
	return &c
}

func decodeChangeEvent(stream IChangeStream, event *bson.M) error {
	err := stream.Decode(event)
	if err != nil {
		return err
	}
	*event = bsonkit.ToMapValue(*event).(bson.M)
	return nil
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

	for i := range out {
		out[i] = bsonkit.ToMapValue(out[i]).(bson.M)
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

		// v2 driver surfaces variadic options as options.Lister[T]; lungo uses *T.
		lister := regexp.MustCompile(`\.\.\.options\.Lister\[go\.mongodb\.org/mongo-driver/v2/mongo/options\.(\w+)\]`)
		f = lister.ReplaceAllString(f, "...*options.$1")

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
