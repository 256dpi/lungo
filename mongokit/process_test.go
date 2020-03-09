package mongokit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func TestProcess(t *testing.T) {
	/* empty */

	ctx := Context{}
	doc := bsonkit.MustConvert(bson.M{})
	query := bsonkit.MustConvert(bson.M{})

	err := Process(ctx, doc, *query, "", true)
	assert.NoError(t, err)

	/* unknown operators */

	query = bsonkit.MustConvert(bson.M{
		"$foo": "bar",
	})

	err = Process(ctx, doc, *query, "", true)
	assert.Error(t, err)
	assert.Equal(t, `unknown top level operator "$foo"`, err.Error())

	err = Process(ctx, doc, *query, "", false)
	assert.Error(t, err)
	assert.Equal(t, `unknown expression operator "$foo"`, err.Error())

	query = bsonkit.MustConvert(bson.M{
		"foo": "bar",
	})

	err = Process(ctx, doc, *query, "", true)
	assert.Error(t, err)
	assert.Equal(t, `missing default operator`, err.Error())

	err = Process(ctx, doc, *query, "", false)
	assert.Error(t, err)
	assert.Equal(t, `missing default operator`, err.Error())
}
