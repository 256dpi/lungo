package bsonkit

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

type missing struct{}

var Missing = missing{}

func Get(doc bson.D, path string) interface{} {
	return get(doc, strings.Split(path, "."))
}

func get(doc bson.D, path []string) interface{} {
	// search for element
	for _, el := range doc {
		if el.Key == path[0] {
			if len(path) == 1 {
				return el.Value
			}

			// check if doc
			if d, ok := el.Value.(bson.D); ok {
				return get(d, path[1:])
			}

			return Missing
		}
	}

	return Missing
}

func Set(doc *bson.D, path string, value interface{}, prepend bool) error {
	return set(doc, strings.Split(path, "."), value, prepend)
}

func set(doc *bson.D, path []string, value interface{}, prepend bool) error {
	// search for element
	for i, el := range *doc {
		if el.Key == path[0] {
			// replace value
			if len(path) == 1 {
				(*doc)[i].Value = value
				return nil
			}

			// check if doc
			if d, ok := el.Value.(bson.D); ok {
				err := set(&d, path[1:], value, prepend)
				if err != nil {
					return err
				}

				// update value
				(*doc)[i].Value = d

				return nil
			}

			return fmt.Errorf("set: cannot add field to %+v", el.Value)
		}
	}

	// add intermediary element
	if len(path) > 1 {
		// prepare object
		d := bson.D{}
		err := set(&d, path[1:], value, prepend)
		if err != nil {
			return err
		}

		// add object
		e := bson.E{Key: path[0], Value: d}
		if prepend {
			*doc = append(bson.D{e}, *doc...)
		} else {
			*doc = append(*doc, e)
		}

		return nil
	}

	// add final element
	e := bson.E{Key: path[0], Value: value}
	if prepend {
		*doc = append(bson.D{e}, *doc...)
	} else {
		*doc = append(*doc, e)
	}

	return nil
}
