package bsonkit

import (
	"go.mongodb.org/mongo-driver/bson"
)

type missing struct{}

var Missing = missing{}

func Get(doc bson.D, field string) interface{} {
	// search for element
	for _, el := range doc {
		if el.Key == field {
			return el.Value
		}
	}

	return Missing
}

func Set(doc bson.D, field string, value interface{}, prepend bool) bson.D {
	// update element in place
	for i, el := range doc {
		if el.Key == field {
			doc[i].Value = value
			return doc
		}
	}

	// create element
	e := bson.E{Key: field, Value: value}

	// prepend or append to document
	if prepend {
		doc = append(bson.D{e}, doc...)
	} else {
		doc = append(doc, e)
	}

	return doc
}
