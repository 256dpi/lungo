package mongokit

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

// TODO: Support array update operators.

type UpdateOperator func(bsonkit.Doc, string, interface{}, bool) error

var FieldUpdateOperators = map[string]UpdateOperator{}

func init() {
	// register field update operators
	FieldUpdateOperators["$set"] = applySet
	FieldUpdateOperators["$setOnInsert"] = applySetOnInsert
	FieldUpdateOperators["$unset"] = applyUnset
	FieldUpdateOperators["$rename"] = applyRename
	FieldUpdateOperators["$inc"] = applyInc
	FieldUpdateOperators["$mul"] = applyMul
	FieldUpdateOperators["$max"] = applyMax
	FieldUpdateOperators["$min"] = applyMin
	FieldUpdateOperators["$currentDate"] = applyCurrentDate
}

func Apply(doc, update bsonkit.Doc, upsert bool) error {
	// process all expressions
	for _, exp := range *update {
		// check operator validity
		if len(exp.Key) == 0 || exp.Key[0] != '$' {
			return fmt.Errorf("apply: expected operator, got %q", exp.Key)
		}

		// lookup operator
		operator := FieldUpdateOperators[exp.Key]
		if operator == nil {
			return fmt.Errorf("apply: unknown operator %q", exp.Key)
		}

		// get object
		obj, ok := exp.Value.(bson.D)
		if !ok {
			return fmt.Errorf("apply: operator expected document")
		}

		// call operator for each pair
		for _, pair := range obj {
			err := operator(doc, pair.Key, pair.Value, upsert)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func applySet(doc bsonkit.Doc, path string, v interface{}, _ bool) error {
	return bsonkit.Put(doc, path, v, false)
}

func applySetOnInsert(doc bsonkit.Doc, path string, v interface{}, upsert bool) error {
	if upsert {
		return bsonkit.Put(doc, path, v, false)
	}

	return nil
}

func applyUnset(doc bsonkit.Doc, path string, _ interface{}, _ bool) error {
	return bsonkit.Unset(doc, path)
}

func applyRename(doc bsonkit.Doc, path string, v interface{}, _ bool) error {
	// get new path
	newPath, ok := v.(string)
	if !ok {
		return fmt.Errorf("apply: $rename: expected string")
	}

	// get old value
	value := bsonkit.Get(doc, path)

	// unset old value
	err := bsonkit.Unset(doc, path)
	if err != nil {
		return err
	}

	// set new value
	err = bsonkit.Put(doc, newPath, value, false)
	if err != nil {
		return err
	}

	return nil
}

func applyInc(doc bsonkit.Doc, path string, v interface{}, _ bool) error {
	return bsonkit.Increment(doc, path, v)
}

func applyMul(doc bsonkit.Doc, path string, v interface{}, _ bool) error {
	return bsonkit.Multiply(doc, path, v)
}

func applyMax(doc bsonkit.Doc, path string, v interface{}, _ bool) error {
	// get value
	value := bsonkit.Get(doc, path)
	if value == bsonkit.Missing {
		return bsonkit.Put(doc, path, v, false)
	}

	// replace value if smaller
	if bsonkit.Compare(value, v) < 0 {
		err := bsonkit.Put(doc, path, v, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func applyMin(doc bsonkit.Doc, path string, v interface{}, _ bool) error {
	// get value
	value := bsonkit.Get(doc, path)
	if value == bsonkit.Missing {
		return bsonkit.Put(doc, path, v, false)
	}

	// replace value if bigger
	if bsonkit.Compare(value, v) > 0 {
		err := bsonkit.Put(doc, path, v, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func applyCurrentDate(doc bsonkit.Doc, path string, v interface{}, _ bool) error {
	// check if boolean
	value, ok := v.(bool)
	if ok {
		// set to time if true
		if value {
			return bsonkit.Put(doc, path, primitive.NewDateTimeFromTime(time.Now()), false)
		}

		return nil
	}

	// coerce object
	obj, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("apply: $currentDate: expected boolean or document")
	}

	// check object
	if len(obj) > 1 || obj[0].Key != "$type" {
		return fmt.Errorf("apply: $currentDate: expected document with a single $type field")
	}

	// set date or timestamp
	switch obj[0].Value {
	case "date":
		return bsonkit.Put(doc, path, primitive.NewDateTimeFromTime(time.Now()), false)
	case "timestamp":
		return bsonkit.Put(doc, path, bsonkit.Generate(), false)
	default:
		return fmt.Errorf("apply: $currentDate: expected $type 'date' or 'timestamp'")
	}
}
