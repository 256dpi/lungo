package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestDecode(t *testing.T) {
	type model struct {
		Title  string
		Clicks int
	}

	var doc model
	err := Decode(&bson.D{
		bson.E{Key: "title", Value: "Hello"},
		bson.E{Key: "clicks", Value: 42},
		bson.E{Key: "foo", Value: false},
	}, &doc)
	assert.NoError(t, err)
	assert.Equal(t, model{
		Title:  "Hello",
		Clicks: 42,
	}, doc)
}

func TestDecodeList(t *testing.T) {
	type model struct {
		Title  string
		Clicks int
	}

	var list []model
	err := DecodeList(List{
		{
			bson.E{Key: "title", Value: "Hello"},
			bson.E{Key: "clicks", Value: 42},
			bson.E{Key: "foo", Value: false},
		},
	}, &list)
	assert.NoError(t, err)
	assert.Equal(t, []model{
		{
			Title:  "Hello",
			Clicks: 42,
		},
	}, list)
}
