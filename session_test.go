package lungo

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestSessionManual(t *testing.T) {
	// commit
	collectionTest(t, func(t *testing.T, c ICollection) {
		ctx := context.Background()

		id1 := primitive.NewObjectID()
		_, err := c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})

		sess, err := c.Database().Client().StartSession()
		assert.NoError(t, err)
		assert.NotNil(t, sess)

		err = sess.StartTransaction()
		assert.NoError(t, err)

		id2 := primitive.NewObjectID()
		err = WithSession(ctx, sess, func(sc ISessionContext) error {
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

		csr, err := c.Find(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))

		err = sess.CommitTransaction(ctx)
		assert.NoError(t, err)

		csr, err = c.Find(ctx, bson.M{})
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
		ctx := context.Background()

		id1 := primitive.NewObjectID()
		_, err := c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})

		sess, err := c.Database().Client().StartSession()
		assert.NoError(t, err)
		assert.NotNil(t, sess)

		err = sess.StartTransaction()
		assert.NoError(t, err)

		id2 := primitive.NewObjectID()
		err = WithSession(ctx, sess, func(sc ISessionContext) error {
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

		csr, err := c.Find(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))

		err = sess.AbortTransaction(ctx)
		assert.NoError(t, err)

		csr, err = c.Find(ctx, bson.M{})
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
		ctx := context.Background()

		id1 := primitive.NewObjectID()
		_, err := c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})

		id2 := primitive.NewObjectID()

		err = c.Database().Client().UseSession(ctx, func(sc ISessionContext) error {
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
	})

	// abort
	collectionTest(t, func(t *testing.T, c ICollection) {
		ctx := context.Background()

		id1 := primitive.NewObjectID()
		_, err := c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})

		id2 := primitive.NewObjectID()

		err = c.Database().Client().UseSession(ctx, func(sc ISessionContext) error {
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

		csr, err := c.Find(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, []bson.M{
			{
				"_id": id1,
				"foo": "bar",
			},
		}, readAll(csr))
	})
}
