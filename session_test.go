package lungo

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSessionManual(t *testing.T) {
	t.Skip()
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
		err = WithSession(nil, sess, func(ctx context.Context) error {
			_, err := c.InsertOne(ctx, bson.M{
				"_id": id2,
				"foo": "bar",
			})
			assert.NoError(t, err)

			csr, err := c.Find(ctx, bson.M{})
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
		err = WithSession(nil, sess, func(ctx context.Context) error {
			_, err := c.InsertOne(ctx, bson.M{
				"_id": id2,
				"foo": "bar",
			})
			assert.NoError(t, err)

			csr, err := c.Find(ctx, bson.M{})
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

		err = c.Database().Client().UseSession(context.TODO(), func(ctx context.Context) error {
			sess := SessionFromContext(ctx)
			assert.NotNil(t, sess)
			_, err = sess.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
				_, err := c.InsertOne(ctx, bson.M{
					"_id": id2,
					"foo": "bar",
				})
				assert.NoError(t, err)

				csr, err := c.Find(ctx, bson.M{})
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

		err = c.Database().Client().UseSession(nil, func(ctx context.Context) error {
			sess, ok := ctx.Value(sessionKey{}).(*Session)
			assert.True(t, ok)
			_, err = sess.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
				_, err := c.InsertOne(ctx, bson.M{
					"_id": id2,
					"foo": "bar",
				})
				assert.NoError(t, err)

				csr, err := c.Find(ctx, bson.M{})
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
