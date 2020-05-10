package mongokit

import (
	"fmt"
	"strconv"
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
	FieldUpdateOperators["$pop"] = applyPop
}

// Changes describes the applied changes to a document.
type Changes struct {
	// Whether the operation was an upsert.
	Upsert bool

	// The fields that have been added or changed in the document.
	Updated map[string]interface{}

	// The fields that have been removed from the document.
	Removed []string
}

// Apply will apply a MongoDB update document on a document using the various
// update operators. The document is updated in place. The changes to the
// document are collected and returned.
func Apply(doc, query, update bsonkit.Doc, upsert bool, arrayFilters bsonkit.List) (*Changes, error) {
	// prepare changes
	changes := &Changes{
		Upsert:  upsert,
		Updated: map[string]interface{}{},
		Removed: []string{},
	}

	// update document according to update
	err := Process(Context{
		Value:                changes,
		TopLevel:             FieldUpdateOperators,
		MultiTopLevel:        true,
		TopLevelArrayFilters: arrayFilters,
		TopLevelQuery:        query,
	}, doc, *update, "", true)
	if err != nil {
		return nil, err
	}

	return changes, nil
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
	if res == bsonkit.Missing {
		return nil
	}

	// record change
	ctx.Value.(*Changes).Removed = append(ctx.Value.(*Changes).Removed, path)

	return nil
}

func applyRename(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// get new path
	newPath, ok := v.(string)
	if !ok {
		return fmt.Errorf("%s: expected string", name)
	}

	// ignore indexed paths
	if IndexedPath(path) || IndexedPath(newPath) {
		return nil
	}

	// unset old value
	value := bsonkit.Unset(doc, path)
	if value == bsonkit.Missing {
		return nil
	}

	// set new value
	_, err := bsonkit.Put(doc, newPath, value, false)
	if err != nil {
		return err
	}

	// record changes
	ctx.Value.(*Changes).Removed = append(ctx.Value.(*Changes).Removed, path)
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
	// TODO: Add support for the modifiers {$each, $slice, $sort, $position}

	// push value
	res, err := bsonkit.Push(doc, path, v)
	if err != nil {
		return err
	}

	// record change if result is an array
	if array, ok := res.(bson.A); ok {
		addedPath := path + "." + strconv.Itoa(len(array)-1)
		ctx.Value.(*Changes).Updated[addedPath] = v
	}

	return nil
}

func applyPop(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// check value
	last := false
	if bsonkit.Compare(v, int64(1)) == 0 {
		last = true
	} else if bsonkit.Compare(v, int64(-1)) != 0 {
		return fmt.Errorf("%s: expected 1 or -1", name)
	}

	// pop element
	res, err := bsonkit.Pop(doc, path, last)
	if err != nil {
		return err
	}

	// check result
	if res == bsonkit.Missing {
		return nil
	}

	// record change
	ctx.Value.(*Changes).Updated[path] = bsonkit.Get(doc, path)

	return nil
}
