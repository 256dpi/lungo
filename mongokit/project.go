package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// ProjectionExpressionOperators defines the available projection operators.
var ProjectionExpressionOperators = map[string]Operator{}

func init() {
	// register expression projection operators
	ProjectionExpressionOperators[""] = projectCondition
	ProjectionExpressionOperators["$slice"] = projectSlice
}

type projectState struct {
	hideID  bool
	include []string
	exclude []string
	merge   map[string]interface{}
}

// ProjectList will apply the provided projection to the specified list.
func ProjectList(list bsonkit.List, projection bsonkit.Doc) (bsonkit.List, error) {
	result := make(bsonkit.List, 0, len(list))
	for _, doc := range list {
		res, err := Project(doc, projection)
		if err != nil {
			return nil, err
		}
		result = append(result, res)
	}
	return result, nil
}

// Project will apply the specified project to the document and return the
// resulting document.
func Project(doc, projection bsonkit.Doc) (bsonkit.Doc, error) {
	// prepare state
	state := projectState{
		merge: map[string]interface{}{},
	}

	// process projection
	err := Process(Context{
		Expression: ProjectionExpressionOperators,
		Value:      &state,
	}, doc, *projection, "", true)
	if err != nil {
		return nil, err
	}

	// validate
	if len(state.include) > 0 && len(state.exclude) > 0 {
		return nil, fmt.Errorf("cannot have a mix of inclusion and exclusion")
	}

	// prepare result
	var res bsonkit.Doc

	// perform inclusion
	if len(state.include) > 0 {
		// set null document
		res = &bson.D{}

		// add id if not hidden
		if !state.hideID {
			_, err := bsonkit.Put(res, "_id", bsonkit.Get(doc, "_id"), false)
			if err != nil {
				return nil, err
			}
		}

		// copy included fields
		for _, path := range state.include {
			value := bsonkit.Get(doc, path)
			if value != bsonkit.Missing {
				_, err = bsonkit.Put(res, path, value, false)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// perform exclusion
	if len(state.exclude) > 0 {
		// clone document
		res = bsonkit.Clone(doc)

		// unset id if not shown
		if state.hideID {
			bsonkit.Unset(res, "_id")
		}

		// unset excluded fields
		for _, path := range state.exclude {
			bsonkit.Unset(res, path)
		}
	}

	// merge fields
	for path, value := range state.merge {
		// check result
		if res == nil {
			// set null document
			res = &bson.D{}

			// set id if not hidden
			if !state.hideID {
				_, err := bsonkit.Put(res, "_id", bsonkit.Get(doc, "_id"), false)
				if err != nil {
					return nil, err
				}
			}
		}

		// add field
		_, err := bsonkit.Put(res, path, value, false)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func projectCondition(ctx Context, _ bsonkit.Doc, _, path string, v interface{}) error {
	// get state
	state := ctx.Value.(*projectState)

	// handle inclusion or exclusion
	if bsonkit.Compare(v, int64(1)) == 0 {
		state.include = append(state.include, path)
	} else if bsonkit.Compare(v, int64(0)) == 0 {
		if path == "_id" {
			state.hideID = true
		} else {
			state.exclude = append(state.include, path)
		}
	} else {
		return fmt.Errorf("invalid projection argument %+v", v)
	}

	return nil
}

func projectSlice(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get state
	state := ctx.Value.(*projectState)

	// coerce number
	var num int
	switch nn := v.(type) {
	case int32:
		num = int(nn)
	case int64:
		num = int(nn)
	case float64:
		num = int(nn)
	default:
		return fmt.Errorf("expected number")
	}

	// get array
	array, ok := bsonkit.Get(doc, path).(bson.A)
	if !ok {
		return nil
	}

	// handle positive
	if num > 0 {
		if num < len(array) {
			state.merge[path] = array[0:num]
		} else {
			state.merge[path] = array
		}
	}

	// handle negative
	if num < 0 {
		num *= -1
		if num < len(array) {
			state.merge[path] = array[len(array)-num:]
		} else {
			state.merge[path] = array
		}
	}

	// handle zero
	if num == 0 {
		state.merge[path] = bson.A{}
	}

	return nil
}
