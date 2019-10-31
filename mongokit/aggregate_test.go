package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func aggregateTest(t *testing.T, list []bson.M, fn func(fn func([]bson.M, interface{}))) {
	t.Run("Mongo", func(t *testing.T) {
		coll := testCollection()

		for _, doc := range list {
			_, err := coll.InsertOne(nil, doc)
			assert.NoError(t, err)
		}

		fn(func(pipeline []bson.M, result interface{}) {
			csr, err := coll.Aggregate(nil, pipeline)
			if _, ok := result.(string); ok {
				assert.Error(t, err)
				assert.Nil(t, csr)
			} else {
				assert.NoError(t, err)
				var out []bson.M
				err = csr.All(nil, &out)
				assert.NoError(t, err)
				assert.Equal(t, result, out)
			}
		})
	})

	t.Run("Lungo", func(t *testing.T) {
		docs := bsonkit.ConvertList(list)

		fn(func(pipeline []bson.M, result interface{}) {
			res, err := Aggregate(docs, bsonkit.ConvertList(pipeline))
			if str, ok := result.(string); ok {
				assert.Error(t, err)
				if err != nil {
					assert.Equal(t, str, err.Error())
				}
				assert.Nil(t, res)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, result, res)
			}
		})
	})
}

func TestAggregate(t *testing.T) {
	aggregateTest(t, []bson.M{
		{"foo": "bar"},
	}, func(fn func([]bson.M, interface{})) {
		// invalid pipeline
		fn([]bson.M{
			{"cool": "cool"},
		}, "lol")
	})
}
