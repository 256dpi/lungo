package mongokit

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// https://github.com/mongodb/mongo/blob/master/src/mongo/db/matcher/expression_leaf.cpp

// TopLevelQueryOperators defines the top level query operators
var TopLevelQueryOperators = map[string]Operator{}

// ExpressionQueryOperators defines the expression query operators.
var ExpressionQueryOperators = map[string]Operator{}

// ErrNotMatched is returned by query operators if the document does not match.
var ErrNotMatched = errors.New("not matched")

func init() {
	// register top level query operators
	TopLevelQueryOperators["$and"] = matchAnd
	TopLevelQueryOperators["$or"] = matchOr
	TopLevelQueryOperators["$nor"] = matchNor
	TopLevelQueryOperators["$jsonSchema"] = matchJSONSchema

	// register expression query operators
	ExpressionQueryOperators[""] = matchComp
	ExpressionQueryOperators["$eq"] = matchComp
	ExpressionQueryOperators["$gt"] = matchComp
	ExpressionQueryOperators["$lt"] = matchComp
	ExpressionQueryOperators["$gte"] = matchComp
	ExpressionQueryOperators["$lte"] = matchComp
	ExpressionQueryOperators["$ne"] = matchComp
	ExpressionQueryOperators["$not"] = matchNot
	ExpressionQueryOperators["$in"] = matchIn
	ExpressionQueryOperators["$nin"] = matchNin
	ExpressionQueryOperators["$exists"] = matchExists
	ExpressionQueryOperators["$type"] = matchType
	ExpressionQueryOperators["$all"] = matchAll
	ExpressionQueryOperators["$size"] = matchSize
	ExpressionQueryOperators["$elemMatch"] = matchElem
}

// Match will test if the specified document matches the supplied MongoDB query
// document.
func Match(doc, query bsonkit.Doc) (bool, error) {
	// match document to query
	err := Process(Context{
		TopLevel:   TopLevelQueryOperators,
		Expression: ExpressionQueryOperators,
	}, doc, *query, "", true)
	if err == ErrNotMatched {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func matchAnd(ctx Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get array
	array, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected array", name)
	}

	// check array
	if len(array) == 0 {
		return fmt.Errorf("%s: empty array", name)
	}

	// match all expressions
	for _, item := range array {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected array of documents", name)
		}

		// match document
		err := Process(ctx, doc, query, "", true)
		if err != nil {
			return err
		}
	}

	return nil
}

func matchOr(ctx Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get array
	array, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected array", name)
	}

	// check array
	if len(array) == 0 {
		return fmt.Errorf("%s: empty array", name)
	}

	// match first item
	for _, item := range array {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected array of documents", name)
		}

		// match document
		err := Process(ctx, doc, query, "", true)
		if err == ErrNotMatched {
			continue
		} else if err != nil {
			return err
		}

		return nil
	}

	return ErrNotMatched
}

func matchNor(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchNegate(func() error {
		return matchOr(ctx, doc, name, path, v)
	})
}

func matchComp(_ Context, doc bsonkit.Doc, op, path string, v interface{}) error {
	return matchUnwind(doc, path, true, false, func(field interface{}) error {
		// determine if comparable (type bracketing)
		lc, _ := bsonkit.Inspect(field)
		rc, _ := bsonkit.Inspect(v)
		comp := lc == rc

		// compare field with value
		res := bsonkit.Compare(field, v)

		// check operator
		var ok bool
		switch op {
		case "", "$eq":
			ok = comp && res == 0
		case "$gt":
			ok = comp && res > 0
		case "$gte":
			ok = comp && res >= 0
		case "$lt":
			ok = comp && res < 0
		case "$lte":
			ok = comp && res <= 0
		case "$ne":
			ok = !comp || res != 0
		default:
			return fmt.Errorf("unknown comparison operator %q", op)
		}
		if !ok {
			return ErrNotMatched
		}

		return nil
	})
}

func matchNot(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// coerce item
	query, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("%s: expected document", name)
	}

	// check document
	if len(query) == 0 {
		return fmt.Errorf("%s: empty document", name)
	}

	// match all expressions
	for _, exp := range query {
		err := ProcessExpression(ctx, doc, path, exp, false)
		if err == ErrNotMatched {
			return nil
		} else if err != nil {
			return err
		}
	}

	// TODO: Support regular expressions.

	return ErrNotMatched
}

func matchIn(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchUnwind(doc, path, true, false, func(field interface{}) error {
		// get array
		array, ok := v.(bson.A)
		if !ok {
			return fmt.Errorf("%s: expected array", name)
		}

		// check if field is in array
		for _, item := range array {
			if bsonkit.Compare(field, item) == 0 {
				return nil
			}
		}

		// TODO: Support regular expressions.

		return ErrNotMatched
	})
}

func matchNin(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchNegate(func() error {
		return matchIn(ctx, doc, name, path, v)
	})
}

func matchExists(_ Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get boolean
	exists := true
	if b, ok := v.(bool); ok {
		exists = b
	}

	// get field value
	field := bsonkit.Get(doc, path)
	if exists {
		if field != bsonkit.Missing {
			return nil
		}

		return ErrNotMatched
	}

	if field == bsonkit.Missing {
		return nil
	}

	return ErrNotMatched
}

func matchType(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// TODO: Support type arrays.
	// TODO: Support array values.

	// check value type
	switch value := v.(type) {
	case string:
		// handle number
		if value == "number" {
			class, _ := bsonkit.Inspect(bsonkit.Get(doc, path))
			if class == bsonkit.Number {
				return nil
			}
			return ErrNotMatched
		}

		// check type string
		vt, ok := bsonkit.Alias2Type[value]
		if !ok {
			return fmt.Errorf("%s: unknown type string", name)
		}

		// match type string
		_, typ := bsonkit.Inspect(bsonkit.Get(doc, path))
		if vt == typ {
			return nil
		}
	case int32, int64, float64:
		// coerce number
		var num byte
		switch nn := v.(type) {
		case int32:
			num = byte(nn)
		case int64:
			num = byte(nn)
		case float64:
			num = byte(nn)
		}

		// check type number
		vt, ok := bsonkit.Number2Type[num]
		if !ok {
			return fmt.Errorf("%s: unknown type number", name)
		}

		// match type number
		_, typ := bsonkit.Inspect(bsonkit.Get(doc, path))
		if vt == typ {
			return nil
		}
	default:
		return fmt.Errorf("%s: expected string or number", name)
	}

	return ErrNotMatched
}

func matchJSONSchema(_ Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get doc
	d, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("%s: expected document", name)
	}

	// evaluate schema
	err := bsonkit.NewSchema(d).Evaluate(*doc)
	if err == bsonkit.ErrValidationFailed {
		return ErrNotMatched
	} else if err != nil {
		return fmt.Errorf("%s: %s", name, err.Error())
	}

	return nil
}

func matchAll(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchUnwind(doc, path, false, true, func(field interface{}) error {
		// get array
		array, ok := v.(bson.A)
		if !ok {
			return fmt.Errorf("%s: expected array", name)
		}

		// check array
		if len(array) == 0 {
			return ErrNotMatched
		}

		// check if array contains array
		if arr, ok := field.(bson.A); ok {
			matches := true
			for _, value := range array {
				ok := false
				for _, element := range arr {
					if bsonkit.Compare(value, element) == 0 {
						ok = true
					}
				}
				if !ok {
					matches = false
				}
			}
			if matches {
				return nil
			}
		}

		// check if field is in array
		for _, item := range array {
			if bsonkit.Compare(field, item) != 0 {
				return ErrNotMatched
			}
		}

		return nil
	})
}

func matchSize(_ Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchUnwind(doc, path, false, false, func(field interface{}) error {
		// check value
		vc, _ := bsonkit.Inspect(v)
		if vc != bsonkit.Number {
			return fmt.Errorf("%s: expected number", name)
		}

		// compare length if array
		array, ok := field.(bson.A)
		if ok {
			if bsonkit.Compare(int64(len(array)), v) == 0 {
				return nil
			}
		}

		return ErrNotMatched
	})
}

func matchElem(ctx Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// get query
	query, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("%s: expected document", name)
	}

	// check query
	if len(query) == 0 {
		return ErrNotMatched
	}

	// get value
	value, _ := bsonkit.All(doc, path, true, true)

	// get array
	array, ok := value.(bson.A)
	if !ok {
		return ErrNotMatched
	}

	// match first item
	for _, item := range array {
		// prepare virtual doc
		virtual := bson.D{
			bson.E{Key: "item", Value: item},
		}

		// TODO: Blacklist unsupported operators.

		// process virtual document
		err := Process(ctx, &virtual, query, "item", false)
		if err == ErrNotMatched {
			continue
		} else if err != nil {
			return err
		}

		return nil
	}

	return ErrNotMatched
}

func matchUnwind(doc bsonkit.Doc, path string, merge, yieldMerge bool, op func(interface{}) error) error {
	// get value
	value, multi := bsonkit.All(doc, path, true, merge)
	if arr, ok := value.(bson.A); ok {
		for _, field := range arr {
			err := op(field)
			if err == ErrNotMatched {
				continue
			} else if err != nil {
				return err
			}

			return nil
		}
	}

	// match value
	if !multi || yieldMerge {
		return op(value)
	}

	return ErrNotMatched
}

func matchNegate(op func() error) error {
	err := op()
	if err == ErrNotMatched {
		return nil
	} else if err != nil {
		return err
	}

	return ErrNotMatched
}
