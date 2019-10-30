package lungo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

func TestStream(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		_, err := c.InsertOne(nil, bson.M{})
		assert.NoError(t, err)

		stream, err := c.Watch(nil, bson.A{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		ret := stream.Next(ctx)
		assert.False(t, ret)

		id1 := primitive.NewObjectID()

		/* insert */

		_, err = c.InsertOne(nil, bson.M{
			"_id": id1,
			"foo": "bar",
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
				"foo": "bar",
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "insert",
		}, event)

		/* replace */

		_, err = c.ReplaceOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"_id": id1,
			"foo": "baz",
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
				"foo": "baz",
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "replace",
		}, event)

		/* update */

		_, err = c.UpdateOne(nil, bson.M{
			"_id": id1,
		}, bson.M{
			"$set": bson.M{
				"foo": "quz",
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
				"foo": "quz",
			},
			"ns": bson.M{
				"db":   c.Database().Name(),
				"coll": c.Name(),
			},
			"operationType": "update",
			"updateDescription": bson.M{
				"updatedFields": bson.M{
					"foo": "quz",
				},
				"removedFields": bson.A{},
			},
		}, event)

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

		/* close */

		err = stream.Close(nil)
		assert.NoError(t, err)

		ret = stream.Next(ctx)
		assert.False(t, ret)

		err = stream.Err()
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

		// invalid operation time
		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetStartAtOperationTime(&primitive.Timestamp{}))
		assert.Error(t, err)
		assert.Nil(t, stream)

		/* prepare */

		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		_, err = c.InsertOne(nil, bson.M{"foo": "bar"})
		assert.NoError(t, err)

		_, err = c.InsertOne(nil, bson.M{"foo": "bar"})
		assert.NoError(t, err)

		ret := stream.Next(nil)
		assert.True(t, ret)

		var event bson.D
		err = stream.Decode(&event)
		assert.NoError(t, err)

		token := bsonkit.Get(&event, "_id")
		timestamp := bsonkit.Get(&event, "clusterTime").(primitive.Timestamp)
		assert.NotEmpty(t, token)
		assert.NotEmpty(t, timestamp)

		/* resume */

		// invalid resume token (resume after)
		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetResumeAfter(token))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		err = stream.Close(nil)
		assert.NoError(t, err)

		// invalid resume token (start after)
		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetStartAfter(token))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		err = stream.Close(nil)
		assert.NoError(t, err)

		// invalid operation time
		stream, err = c.Watch(nil, bson.A{}, options.ChangeStream().SetStartAtOperationTime(&timestamp))
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		err = stream.Close(nil)
		assert.NoError(t, err)

		// TODO: Check returned first event.
	})
}

// TODO: Test stream invalidation and resumption capabilities.
