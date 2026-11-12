package lungo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSessionManual(t *testing.T) {
	// commit
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		_, err := c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})
		assert.NoError(t, err)

		sess, err := c.Database().Client().StartSession()
		assert.NoError(t, err)
		assert.NotNil(t, sess)

		err = sess.StartTransaction()
		assert.NoError(t, err)

		id2 := bson.NewObjectID()
		err = WithSession(nil, sess, func(sc ISessionContext) error {
			_, err := c.InsertOne(sc, bson.M{
				"_id": id2,
				"foo": "bar",
			})
			assert.NoError(t, err)

			csr, err := c.Find(sc, bson.M{})
			assert.NoError(t, err)
			assert.Equal(t, []bson.M{
				{
					"_id": id1,
					"foo": "bar",
				},
				{
					"_id": id2,
					"foo": "bar",
				},
			}, readAll(csr))

			return nil
		})
		assert.NoError(t, err)

		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))

		err = sess.CommitTransaction(nil)
		assert.NoError(t, err)

		csr, err = c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "bar",
			},
		}, readAll(csr))
	})

	// abort
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		_, err := c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})
		assert.NoError(t, err)

		sess, err := c.Database().Client().StartSession()
		assert.NoError(t, err)
		assert.NotNil(t, sess)

		err = sess.StartTransaction()
		assert.NoError(t, err)

		id2 := bson.NewObjectID()
		err = WithSession(nil, sess, func(sc ISessionContext) error {
			_, err := c.InsertOne(sc, bson.M{
				"_id": id2,
				"foo": "bar",
			})
			assert.NoError(t, err)

			csr, err := c.Find(sc, bson.M{})
			assert.NoError(t, err)
			assert.Equal(t, []bson.M{
				{
					"_id": id1,
					"foo": "bar",
				},
				{
					"_id": id2,
					"foo": "bar",
				},
			}, readAll(csr))

			return nil
		})
		assert.NoError(t, err)

		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))

		err = sess.AbortTransaction(nil)
		assert.NoError(t, err)

		csr, err = c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))
	})
}

func TestSessionAutomatic(t *testing.T) {
	// commit
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		_, err := c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})

		id2 := bson.NewObjectID()

		err = c.Database().Client().UseSession(nil, func(sc ISessionContext) error {
			_, err = sc.WithTransaction(sc, func(sc ISessionContext) (interface{}, error) {
				_, err := c.InsertOne(sc, bson.M{
					"_id": id2,
					"foo": "bar",
				})
				assert.NoError(t, err)

				csr, err := c.Find(sc, bson.M{})
				assert.NoError(t, err)
				assert.Equal(t, []bson.M{
					{
						"_id": id1,
						"foo": "bar",
					},
					{
						"_id": id2,
						"foo": "bar",
					},
				}, readAll(csr))

				return nil, nil
			})

			return err
		})
		assert.NoError(t, err)

		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
			{
				"_id": id2,
				"foo": "bar",
			},
		}, readAll(csr))
	})

	// abort
	collectionTest(t, func(t *testing.T, c ICollection) {
		id1 := bson.NewObjectID()
		_, err := c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})

		id2 := bson.NewObjectID()

		err = c.Database().Client().UseSession(nil, func(sc ISessionContext) error {
			_, err = sc.WithTransaction(sc, func(sc ISessionContext) (interface{}, error) {
				_, err := c.InsertOne(sc, bson.M{
					"_id": id2,
					"foo": "bar",
				})
				assert.NoError(t, err)

				csr, err := c.Find(sc, bson.M{})
				assert.NoError(t, err)
				assert.Equal(t, []bson.M{
					{
						"_id": id1,
						"foo": "bar",
					},
					{
						"_id": id2,
						"foo": "bar",
					},
				}, readAll(csr))

				return nil, fmt.Errorf("foo")
			})

			return err
		})
		assert.Error(t, err)

		csr, err := c.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))
	})
}

func TestSessionStartTransactionConcurrent(t *testing.T) {
	// concurrent StartTransaction calls on the same session must not both
	// succeed; at most one can win, the rest must report an existing
	// transaction error
	const goroutines = 8
	const rounds = 32

	for r := 0; r < rounds; r++ {
		sess, err := testLungoClient.StartSession()
		assert.NoError(t, err)

		var success int64
		var existing int64
		var wg sync.WaitGroup
		ready := make(chan struct{})
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				<-ready
				err := sess.StartTransaction()
				switch {
				case err == nil:
					atomic.AddInt64(&success, 1)
				case err.Error() == "existing transaction":
					atomic.AddInt64(&existing, 1)
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		close(ready)
		wg.Wait()

		assert.Equal(t, int64(1), atomic.LoadInt64(&success), "round %d: expected exactly one StartTransaction to succeed", r)
		assert.Equal(t, int64(goroutines-1), atomic.LoadInt64(&existing), "round %d: expected the rest to report existing transaction", r)

		_ = sess.AbortTransaction(nil)
		sess.EndSession(nil)
	}
}

func TestSessionListOpsHonorTransaction(t *testing.T) {
	d := testLungoClient.Database(testDB)

	// pre-existing collection so the database exists
	_, err := d.Collection("session-list-pre").InsertOne(nil, bson.M{"x": 1})
	assert.NoError(t, err)

	err = d.Client().UseSession(nil, func(sc ISessionContext) error {
		_, err := sc.WithTransaction(sc, func(sc ISessionContext) (interface{}, error) {
			// create a new collection inside the transaction
			_, err := d.Collection("session-list-new").InsertOne(sc, bson.M{"x": 1})
			assert.NoError(t, err)

			// ListCollectionNames inside the transaction must see it
			names, err := d.ListCollectionNames(sc, bson.M{})
			assert.NoError(t, err)
			assert.Contains(t, names, "session-list-new")

			return nil, nil
		})
		return err
	})
	assert.NoError(t, err)
}
