package bsonkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestConvert(t *testing.T) {
	res := MustConvert(bson.M{
		"foo": "bar",
		"bar": bson.A{
			bson.M{
				"foo": "bar",
			},
		},
		"baz": bson.D{
			bson.E{Key: "foo", Value: bson.M{
				"foo": "bar",
			}},
		},
	})
	assert.Equal(t, &bson.D{
		bson.E{Key: "bar", Value: bson.A{
			bson.D{
				bson.E{Key: "foo", Value: "bar"},
			},
		}},
		bson.E{Key: "baz", Value: bson.D{
			bson.E{Key: "foo", Value: bson.D{
				bson.E{Key: "foo", Value: "bar"},
			}},
		}},
		bson.E{Key: "foo", Value: "bar"},
	}, res)

	doc, err := Convert(bson.M{
		"foo": uint(1),
	})
	assert.Error(t, err)
	assert.Nil(t, doc)
	assert.Equal(t, "unsupported type uint", err.Error())

	assert.Panics(t, func() {
		MustConvert(bson.M{
			"foo": uint(1),
		})
	})
}

func TestConvertList(t *testing.T) {
	res := MustConvertList([]bson.M{
		{
			"foo": "bar",
		},
		{
			"baz": bson.D{
				bson.E{Key: "foo", Value: bson.M{
					"foo": "bar",
				}},
			},
		},
	})
	assert.Equal(t, List{
		&bson.D{
			bson.E{Key: "foo", Value: "bar"},
		},
		&bson.D{
			bson.E{Key: "baz", Value: bson.D{
				bson.E{Key: "foo", Value: bson.D{
					bson.E{Key: "foo", Value: "bar"},
				}},
			}},
		},
	}, res)
}

func TestConvertValue(t *testing.T) {
	now := time.Now()
	res := MustConvertValue(now)
	assert.Equal(t, primitive.NewDateTimeFromTime(now), res)
}
