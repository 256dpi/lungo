package mongokit

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/256dpi/lungo/bsonkit"
)

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
	FieldUpdateOperators["$push"] = applyPush

	// wrap all operators
	for name, operator := range FieldUpdateOperators {
		FieldUpdateOperators[name] = applyAll(name, operator)
	}
}

// Changes describes the applied changes to a document.
type Changes struct {
	Upsert  bool
	Updated map[string]interface{}
	Removed map[string]interface{}
}

// Apply will apply a MongoDB update document on a document using the various
// update operators. The document is updated in place. The changes to the
// document are collected and returned.
func Apply(doc, update bsonkit.Doc, upsert bool) (*Changes, error) {
	// prepare changes
	changes := &Changes{
		Upsert:  upsert,
		Updated: map[string]interface{}{},
		Removed: map[string]interface{}{},
	}

	// update document according to update
	err := Process(Context{
		Value:    changes,
		TopLevel: FieldUpdateOperators,
	}, doc, *update, "", true)
	if err != nil {
		return nil, err
	}

	return changes, nil
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

func applySet(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// set new value
	_, err := bsonkit.Put(doc, path, v, false)
	if err != nil {
		return err
	}

	// record change
	ctx.Value.(*Changes).Updated[path] = v

	return nil
}

func applySetOnInsert(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// check if upsert
	if !ctx.Value.(*Changes).Upsert {
		return nil
	}

	// set new value
	_, err := bsonkit.Put(doc, path, v, false)
	if err != nil {
		return err
	}

	// record change
	ctx.Value.(*Changes).Updated[path] = v

	return nil
}

func applyUnset(ctx Context, doc bsonkit.Doc, _, path string, _ interface{}) error {
	// remove value
	res := bsonkit.Unset(doc, path)

	// record change if value existed
	if res != bsonkit.Missing {
		ctx.Value.(*Changes).Removed[path] = res
	}

	return nil
}

func applyRename(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
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
	res := bsonkit.Unset(doc, path)

	// record change if value existed
	if res != bsonkit.Missing {
		ctx.Value.(*Changes).Removed[path] = res
	}

	// set new value
	_, err := bsonkit.Put(doc, newPath, value, false)
	if err != nil {
		return err
	}

	// record change
	ctx.Value.(*Changes).Updated[newPath] = value

	return nil
}

func applyInc(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// increment value
	res, err := bsonkit.Increment(doc, path, v)
	if err != nil {
		return err
	}

	// record change
	ctx.Value.(*Changes).Updated[path] = res

	return nil
}

func applyMul(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// multiply value
	res, err := bsonkit.Multiply(doc, path, v)
	if err != nil {
		return err
	}

	// record change
	ctx.Value.(*Changes).Updated[path] = res

	return nil
}

func applyMax(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get value
	value := bsonkit.Get(doc, path)
	if value == bsonkit.Missing {
		// set value
		_, err := bsonkit.Put(doc, path, v, false)
		if err != nil {
			return err
		}

		// record change
		ctx.Value.(*Changes).Updated[path] = v

		return nil
	}

	// replace value if smaller
	if bsonkit.Compare(value, v) < 0 {
		// replace value
		_, err := bsonkit.Put(doc, path, v, false)
		if err != nil {
			return err
		}

		// record change
		ctx.Value.(*Changes).Updated[path] = v
	}

	return nil
}

func applyMin(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get value
	value := bsonkit.Get(doc, path)
	if value == bsonkit.Missing {
		// set value
		_, err := bsonkit.Put(doc, path, v, false)
		if err != nil {
			return err
		}

		// record change
		ctx.Value.(*Changes).Updated[path] = v

		return nil
	}

	// replace value if bigger
	if bsonkit.Compare(value, v) > 0 {
		// replace value
		_, err := bsonkit.Put(doc, path, v, false)
		if err != nil {
			return err
		}

		// record change
		ctx.Value.(*Changes).Updated[path] = v
	}

	return nil
}

func applyCurrentDate(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// check if boolean
	value, ok := v.(bool)
	if ok {
		// set to time if true
		if value {
			// get time
			now := primitive.NewDateTimeFromTime(time.Now().UTC())

			// set time
			_, err := bsonkit.Put(doc, path, now, false)
			if err != nil {
				return err
			}

			// record change
			ctx.Value.(*Changes).Updated[path] = now
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

	// get value
	var now interface{}
	switch args[0].Value {
	case "date":
		now = primitive.NewDateTimeFromTime(time.Now().UTC())
	case "timestamp":
		now = bsonkit.Now()
	default:
		return fmt.Errorf("%s: expected $type 'date' or 'timestamp'", name)
	}

	// set value
	_, err := bsonkit.Put(doc, path, now, false)
	if err == nil {
		return err
	}

	// record change
	ctx.Value.(*Changes).Updated[path] = now

	return nil
}

func applyPush(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// TODO: add support for the modifiers {$each, $slice, $sort, $position}

	// add value
	res, err := bsonkit.Push(doc, path, v)
	if err != nil {
		return err
	}

	// record change
	ctx.Value.(*Changes).Updated[path] = res

	return nil
}
