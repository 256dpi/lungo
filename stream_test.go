package lungo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestStream(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		stream, err := c.Watch(nil, bson.A{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		ret := stream.TryNext(timeout(50))
		assert.False(t, ret)

		id1 := primitive.NewObjectID()

		/* insert */

		_, err = c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": bson.M{
				"bar": "baz",
			},
		})
		assert.NoError(t, err)

		ret = stream.Next(nil)
		assert.True(t, ret)

		var event bson.M
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": bson.M{
					"bar": "baz",
				},
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "insert",
		}, event)

		ret = stream.TryNext(timeout(50))
		assert.False(t, ret)

		/* replace */

		_, err = c.ReplaceOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"_id": id1,
			"foo": bson.M{
				"bar": "quz",
			},
		})
		assert.NoError(t, err)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": bson.M{
					"bar": "quz",
				},
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "replace",
		}, event)

		ret = stream.TryNext(timeout(50))
		assert.False(t, ret)

		/* update (change field) */

		_, err = c.UpdateOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"$set": bson.M{
				"foo.bar": "baz",
			},
		})
		assert.NoError(t, err)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": bson.M{
					"bar": "baz",
				},
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "update",
			"updateDescription": bson.M{
				"updatedFields": bson.M{
					"foo.bar": "baz",
				},
				"removedFields": bson.A{},
			},
		}, event)

		ret = stream.TryNext(timeout(50))
		assert.False(t, ret)

		/* update (remove field) */

		_, err = c.UpdateOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"$unset": bson.M{
				"foo.bar": "",
			},
		})
		assert.NoError(t, err)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": bson.M{},
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "update",
			"updateDescription": bson.M{
				"updatedFields": bson.M{},
				"removedFields": bson.A{
					"foo.bar",
				},
			},
		}, event)

		ret = stream.TryNext(timeout(50))
		assert.False(t, ret)

		/* delete */

		_, err = c.DeleteOne(nil, bson.M{
			"_id": id1,
		})
		assert.NoError(t, err)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "delete",
		}, event)

		ret = stream.TryNext(timeout(50))
		assert.False(t, ret)

		/* close */

		err = stream.Close(nil)
		assert.NoError(t, err)

		ret = stream.Next(timeout(50))
		assert.False(t, ret)

		err = stream.Err()
		assert.NoError(t, err)
	})
}

func TestStreamArrayChanges(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		stream, err := c.Watch(nil, bson.A{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		ret := stream.TryNext(timeout(50))
		assert.False(t, ret)

		id1 := primitive.NewObjectID()

		/* insert */

		_, err = c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": bson.A{
				bson.M{
					"foo": "bar",
				},
			},
		})
		assert.NoError(t, err)

		ret = stream.Next(nil)
		assert.True(t, ret)

		var event bson.M
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": bson.A{
					bson.M{
						"foo": "bar",
					},
				},
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "insert",
		}, event)

		ret = stream.TryNext(timeout(50))
		assert.False(t, ret)

		/* update (change field) */

		_, err = c.UpdateOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"$push": bson.M{
				"foo": bson.M{
					"bar": "baz",
				},
			},
		})
		assert.NoError(t, err)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": bson.A{
					bson.M{
						"foo": "bar",
					},
					bson.M{
						"bar": "baz",
					},
				},
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "update",
			"updateDescription": bson.M{
				"updatedFields": bson.M{
					"foo.1": bson.M{
						"bar": "baz",
					},
				},
				"removedFields": bson.A{},
			},
		}, event)

		ret = stream.TryNext(timeout(50))
		assert.False(t, ret)

		/* update (remove field) */

		_, err = c.UpdateOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			// TODO: Test with $pop.
			"$unset": bson.M{
				"foo.0": "",
			},
		})
		assert.NoError(t, err)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": bson.A{
					nil,
					bson.M{
						"bar": "baz",
					},
				},
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "update",
			"updateDescription": bson.M{
				"updatedFields": bson.M{},
				"removedFields": bson.A{
					"foo.0",
				},
			},
		}, event)

		ret = stream.TryNext(timeout(50))
		assert.False(t, ret)

		/* close */

		err = stream.Close(nil)
		assert.NoError(t, err)

		ret = stream.Next(timeout(50))
		assert.False(t, ret)

		err = stream.Err()
		assert.NoError(t, err)
	})
}

func TestStreamAsync(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		stream, err := c.Watch(nil, bson.A{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		go func() {
			time.Sleep(50 * time.Millisecond)

			_, err = c.InsertOne(nil, bson.M{"foo": "bar"})
			assert.NoError(t, err)

			time.Sleep(50 * time.Millisecond)

			_, err = c.InsertOne(nil, bson.M{"foo": "bar"})
			assert.NoError(t, err)
		}()

		ret := stream.Next(nil)
		assert.True(t, ret)

		ret = stream.Next(nil)
		assert.True(t, ret)

		err = stream.Close(nil)
		assert.NoError(t, err)
	})
}

func TestStreamResumption(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		/* invalid token and time */

		// invalid resume token (resume after)
		stream, err := c.Watch(nil, bson.A{}, options.ChangeStream().SetResumeAfter(bson.M{}))
		assert.Error(t, err)
		assert.Nil(t, stream)

		// invalid resume token (start after)
		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetStartAfter(bson.M{}))
		assert.Error(t, err)
		assert.Nil(t, stream)

		/* prepare */

		stream, err = c.Watch(nil, bson.A{})
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		id1 := primitive.NewObjectID()
		_, err = c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})
		assert.NoError(t, err)

		id2 := primitive.NewObjectID()
		_, err = c.InsertOne(nil, bson.M{
			"_id": id2,
			"foo": "bar",
		})
		assert.NoError(t, err)

		ret := stream.Next(nil)
		assert.True(t, ret)

		var event bson.M
		err = stream.Decode(&event)
		assert.NoError(t, err)

		token := event["_id"]
		timestamp := event["clusterTime"].(primitive.Timestamp)
		assert.NotEmpty(t, token)
		assert.NotEmpty(t, timestamp)

		/* resume after */

		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetResumeAfter(token))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id2,
			},
			"fullDocument": bson.M{
				"_id": id2,
				"foo": "bar",
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "insert",
		}, event)

		err = stream.Close(nil)
		assert.NoError(t, err)

		/* start after */

		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetStartAfter(token))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id2,
			},
			"fullDocument": bson.M{
				"_id": id2,
				"foo": "bar",
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "insert",
		}, event)

		err = stream.Close(nil)
		assert.NoError(t, err)

		/* start at */

		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetStartAtOperationTime(&timestamp))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": "bar",
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "insert",
		}, event)

		err = stream.Close(nil)
		assert.NoError(t, err)
	})
}

func TestStreamInvalidationCollection(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		stream, err := c.Watch(nil, bson.A{})
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		/* drop */

		err = c.Drop(nil)
		assert.NoError(t, err)

		ret := stream.Next(nil)
		assert.True(t, ret)

		var event bson.M
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "drop",
		}, event)

		/* invalidate */

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":           event["_id"],
			"clusterTime":   event["clusterTime"],
			"operationType": "invalidate",
		}, event)

		/* close */

		err = stream.Close(nil)
		assert.NoError(t, err)

		err = stream.Err()
		assert.NoError(t, err)
	})
}

func TestStreamInvalidationDatabase(t *testing.T) {
	clientTest(t, func(t *testing.T, c IClient) {
		db := c.Database("test-lungo-stream")

		_, err := db.Collection("foo").InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		stream, err := db.Watch(nil, bson.A{})
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		/* drop */

		err = db.Drop(nil)
		assert.NoError(t, err)

		ret := stream.Next(nil)
		assert.True(t, ret)

		var event bson.M
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"ns": bson.M{
				"db":   db.Name(),
				"coll": "foo",
			},
			"operationType": "drop",
		}, event)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"ns": bson.M{
				"db": db.Name(),
			},
			"operationType": "dropDatabase",
		}, event)

		/* invalidate */

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":           event["_id"],
			"clusterTime":   event["clusterTime"],
			"operationType": "invalidate",
		}, event)

		/* close */

		err = stream.Close(nil)
		assert.NoError(t, err)

		err = stream.Err()
		assert.NoError(t, err)
	})
}

func TestStreamInvalidationClient(t *testing.T) {
	clientTest(t, func(t *testing.T, c IClient) {
		db := c.Database("test-lungo-stream")

		_, err := db.Collection("foo").InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		stream, err := c.Watch(nil, bson.A{})
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		/* drop */

		err = db.Drop(nil)
		assert.NoError(t, err)

		ret := stream.Next(nil)
		assert.True(t, ret)

		var event bson.M
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"ns": bson.M{
				"db":   db.Name(),
				"coll": "foo",
			},
			"operationType": "drop",
		}, event)

		ret = stream.Next(nil)
		assert.True(t, ret)

		event = nil
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"ns": bson.M{
				"db": db.Name(),
			},
			"operationType": "dropDatabase",
		}, event)

		/* invalidate */

		ret = stream.Next(timeout(50))
		assert.False(t, ret)

		/* close */

		err = stream.Close(nil)
		assert.NoError(t, err)

		err = stream.Err()
		assert.NoError(t, err)
	})
}

func TestStreamIsolationCollection(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		stream, err := c.Watch(nil, bson.A{})
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		_, err = c.Database().Collection("foo").InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		id1 := primitive.NewObjectID()
		_, err = c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})
		assert.NoError(t, err)

		ret := stream.Next(nil)
		assert.True(t, ret)

		var event bson.M
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": "bar",
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "insert",
		}, event)

		err = stream.Close(nil)
		assert.NoError(t, err)

		err = stream.Err()
		assert.NoError(t, err)
	})
}

func TestStreamIsolationDatabase(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		stream, err := c.Database().Watch(nil, bson.A{})
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		_, err = c.Database().Client().Database("test-lungo-stream").Collection("foo").InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		id1 := primitive.NewObjectID()
		_, err = c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
		})
		assert.NoError(t, err)

		ret := stream.Next(nil)
		assert.True(t, ret)

		var event bson.M
		err = stream.Decode(&event)
		assert.NoError(t, err)
		assert.NotEmpty(t, event["_id"])
		assert.NotEmpty(t, event["clusterTime"])
		assert.Equal(t, bson.M{
			"_id":         event["_id"],
			"clusterTime": event["clusterTime"],
			"documentKey": bson.M{
				"_id": id1,
			},
			"fullDocument": bson.M{
				"_id": id1,
				"foo": "bar",
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "insert",
		}, event)

		err = stream.Close(nil)
		assert.NoError(t, err)

		err = stream.Err()
		assert.NoError(t, err)
	})
}
