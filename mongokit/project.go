package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// ProjectionExpressionOperators defines the available projection operators.
var ProjectionExpressionOperators = map[string]Operator{}

func init() {
	// register expression projection operators
	ProjectionExpressionOperators[""] = projectCondition
	ProjectionExpressionOperators["$slice"] = projectSlice
	ProjectionExpressionOperators["$elemMatch"] = projectElemMatch
}

type projectState struct {
	hideID  bool
	include []string
	exclude []string
	merge   map[string]interface{}
	skip    map[string]bool
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
		skip:  map[string]bool{},
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

		// copy id
		_, err := bsonkit.Put(res, "_id", bsonkit.Get(doc, "_id"), false)
		if err != nil {
			return nil, err
		}

		// copy included fields
		for _, path := range state.include {
			// skip paths that should not be copied from the original
			// document (e.g. $elemMatch overrides, where the merge step
			// supplies the value or leaves it absent on no match)
			if state.skip[path] {
				continue
			}
			value := bsonkit.Get(doc, path)
			if value != bsonkit.Missing {
				_, err = bsonkit.Put(res, path, value, false)
				if err != nil {
					return nil, err
				}
			}
		}
	} else {
		// no inclusion: start from a full clone so operator-only projections
		// (e.g. $slice without inclusion fields) preserve every other field
		res = bsonkit.Clone(doc)

		// apply exclusions on the clone
		for _, path := range state.exclude {
			bsonkit.Unset(res, path)
		}
	}

	// merge fields (overlays from operator expressions)
	for path, value := range state.merge {
		_, err := bsonkit.Put(res, path, value, false)
		if err != nil {
			return nil, err
		}
	}

	// hide id
	if state.hideID {
		bsonkit.Unset(res, "_id")
	}

	return res, nil
}

func projectCondition(ctx Context, _ bsonkit.Doc, _, path string, v interface{}) error {
	// get state
	state := ctx.Value.(*projectState)

	// determine inclusion or exclusion (accept both numeric 0/1 and bool)
	var include bool
	switch b := v.(type) {
	case bool:
		include = b
	default:
		if bsonkit.Compare(v, int64(1)) == 0 {
			include = true
		} else if bsonkit.Compare(v, int64(0)) == 0 {
			include = false
		} else {
			return fmt.Errorf("invalid projection argument %+v", v)
		}
	}

	// handle inclusion or exclusion
	if include {
		state.include = append(state.include, path)
	} else if path == "_id" {
		state.hideID = true
	} else {
		state.exclude = append(state.exclude, path)
	}

	return nil
}

func projectSlice(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get state
	state := ctx.Value.(*projectState)

	// parse argument: either a single number (limit-only) or a [skip, limit]
	// array
	var skip, limit int
	var hasSkip bool
	switch nn := v.(type) {
	case int32:
		limit = int(nn)
	case int64:
		limit = int(nn)
	case float64:
		limit = int(nn)
	case bson.A:
		if len(nn) != 2 {
			return fmt.Errorf("$slice: array argument requires 2 elements, got %d", len(nn))
		}
		s, ok := projectSliceInt(nn[0])
		if !ok {
			return fmt.Errorf("$slice: skip must be a number")
		}
		l, ok := projectSliceInt(nn[1])
		if !ok {
			return fmt.Errorf("$slice: limit must be a number")
		}
		if l < 0 {
			return fmt.Errorf("$slice: limit must be non-negative")
		}
		skip = s
		limit = l
		hasSkip = true
	default:
		return fmt.Errorf("expected number or array")
	}

	// get array
	array, ok := bsonkit.Get(doc, path).(bson.A)
	if !ok {
		return nil
	}

	// handle [skip, limit] form
	if hasSkip {
		n := len(array)
		var start int
		if skip < 0 {
			start = n + skip
			if start < 0 {
				start = 0
			}
		} else {
			start = skip
			if start > n {
				start = n
			}
		}
		end := start + limit
		if end > n {
			end = n
		}
		state.merge[path] = append(bson.A{}, array[start:end]...)
		return nil
	}

	// limit-only form
	switch {
	case limit > 0:
		if limit < len(array) {
			state.merge[path] = array[0:limit]
		} else {
			state.merge[path] = array
		}
	case limit < 0:
		n := -limit
		if n < len(array) {
			state.merge[path] = array[len(array)-n:]
		} else {
			state.merge[path] = array
		}
	default:
		state.merge[path] = bson.A{}
	}

	return nil
}

func projectSliceInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

func projectElemMatch(ctx Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get state
	state := ctx.Value.(*projectState)

	// get query
	query, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("$elemMatch: expected document")
	}

	// $elemMatch is inclusion-style: the projection only emits _id and the
	// targeted field. Mark the path as included but skipped so the
	// inclusion phase does not copy the original array.
	state.include = append(state.include, path)
	state.skip[path] = true

	// get array (non-array values: field is omitted)
	array, ok := bsonkit.Get(doc, path).(bson.A)
	if !ok {
		return nil
	}

	// build a query context for matching individual array elements
	queryCtx := Context{
		TopLevel:   TopLevelQueryOperators,
		Expression: ExpressionQueryOperators,
	}

	// find first matching element
	for _, item := range array {
		virtual := bson.D{
			bson.E{Key: "item", Value: item},
		}
		err := Process(queryCtx, &virtual, query, "item", false)
		if err == ErrNotMatched {
			continue
		} else if err != nil {
			return err
		}

		// emit single-element array via merge
		state.merge[path] = bson.A{item}

		return nil
	}

	return nil
}
