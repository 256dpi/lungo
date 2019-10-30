package mongokit

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

// TODO: Support array update operators.

// FieldUpdateOperators defines the field update operators.
var FieldUpdateOperators = map[string]Operator{}

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

	// wrap all operators
	for name, operator := range FieldUpdateOperators {
		FieldUpdateOperators[name] = applyAll(name, operator)
	}
}

// Apply will apply a MongoDB update document on a document using the various
// update operators. The document is updated in place.
func Apply(doc, update bsonkit.Doc, upsert bool) error {
	// update document according to update
	return Process(Context{
		Value:    upsert,
		TopLevel: FieldUpdateOperators,
	}, doc, *update, "", true)
}

func applyAll(name string, operator Operator) Operator {
	return func(ctx Context, doc bsonkit.Doc, op, path string, v interface{}) error {
		// get update document
		update, ok := v.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected document", name)
		}

		// call operator for each pair
		for _, pair := range update {
			err := operator(ctx, doc, op, pair.Key, pair.Value)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func applySet(_ Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	_, err := bsonkit.Put(doc, path, v, false)
	return err
}

func applySetOnInsert(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	if ctx.Value.(bool) {
		_, err := bsonkit.Put(doc, path, v, false)
		return err
	}

	return nil
}

func applyUnset(_ Context, doc bsonkit.Doc, _, path string, _ interface{}) error {
	bsonkit.Unset(doc, path)
	return nil
}

func applyRename(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// get new path
	newPath, ok := v.(string)
	if !ok {
		return fmt.Errorf("%s: expected string", name)
	}

	// get old value
	value := bsonkit.Get(doc, path)
	if value == bsonkit.Missing {
		return nil
	}

	// unset old value
	bsonkit.Unset(doc, path)

	// set new value
	_, err := bsonkit.Put(doc, newPath, value, false)
	if err != nil {
		return err
	}

	return nil
}

func applyInc(_ Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	return bsonkit.Increment(doc, path, v)
}

func applyMul(_ Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	return bsonkit.Multiply(doc, path, v)
}

func applyMax(_ Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get value
	value := bsonkit.Get(doc, path)
	if value == bsonkit.Missing {
		_, err := bsonkit.Put(doc, path, v, false)
		return err
	}

	// replace value if smaller
	if bsonkit.Compare(value, v) < 0 {
		_, err := bsonkit.Put(doc, path, v, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func applyMin(_ Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get value
	value := bsonkit.Get(doc, path)
	if value == bsonkit.Missing {
		_, err := bsonkit.Put(doc, path, v, false)
		return err
	}

	// replace value if bigger
	if bsonkit.Compare(value, v) > 0 {
		_, err := bsonkit.Put(doc, path, v, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func applyCurrentDate(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// check if boolean
	value, ok := v.(bool)
	if ok {
		// set to time if true
		if value {
			_, err := bsonkit.Put(doc, path, primitive.NewDateTimeFromTime(time.Now()), false)
			return err
		}

		return nil
	}

	// coerce document
	args, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("%s: expected boolean or document", name)
	}

	// check document
	if len(args) > 1 || args[0].Key != "$type" {
		return fmt.Errorf("%s: expected document with a single $type field", name)
	}

	// set date or timestamp
	switch args[0].Value {
	case "date":
		_, err := bsonkit.Put(doc, path, primitive.NewDateTimeFromTime(time.Now()), false)
		return err
	case "timestamp":
		_, err := bsonkit.Put(doc, path, bsonkit.Now(), false)
		return err
	default:
		return fmt.Errorf("%s: expected $type 'date' or 'timestamp'", name)
	}
}
