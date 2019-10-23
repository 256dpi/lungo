package mongokit

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

// TODO: Support array update operators.

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

func Apply(doc, update bsonkit.Doc, upsert bool) error {
	// update document according to update
	return Process(&Context{
		Upsert:   upsert,
		TopLevel: FieldUpdateOperators,
	}, doc, *update, true)
}

func applyAll(name string, operator Operator) Operator {
	return func(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
		// get object
		obj, ok := v.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected document", name)
		}

		// call operator for each pair
		for _, pair := range obj {
			err := operator(ctx, doc, pair.Key, pair.Value)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func applySet(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
	return bsonkit.Put(doc, path, v, false)
}

func applySetOnInsert(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
	if ctx.Upsert {
		return bsonkit.Put(doc, path, v, false)
	}

	return nil
}

func applyUnset(ctx *Context, doc bsonkit.Doc, path string, _ interface{}) error {
	return bsonkit.Unset(doc, path)
}

func applyRename(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
	// get new path
	newPath, ok := v.(string)
	if !ok {
		return fmt.Errorf("$rename: expected string")
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

func applyInc(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
	return bsonkit.Increment(doc, path, v)
}

func applyMul(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
	return bsonkit.Multiply(doc, path, v)
}

func applyMax(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
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

func applyMin(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
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

func applyCurrentDate(ctx *Context, doc bsonkit.Doc, path string, v interface{}) error {
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
		return fmt.Errorf("$currentDate: expected boolean or document")
	}

	// check object
	if len(obj) > 1 || obj[0].Key != "$type" {
		return fmt.Errorf("$currentDate: expected document with a single $type field")
	}

	// set date or timestamp
	switch obj[0].Value {
	case "date":
		return bsonkit.Put(doc, path, primitive.NewDateTimeFromTime(time.Now()), false)
	case "timestamp":
		return bsonkit.Put(doc, path, bsonkit.Generate(), false)
	default:
		return fmt.Errorf("$currentDate: expected $type 'date' or 'timestamp'")
	}
}
