package mongokit

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// https://github.com/mongodb/mongo/blob/master/src/mongo/db/matcher/expression_leaf.cpp

var TopLevelQueryOperators = map[string]Operator{}
var ExpressionQueryOperators = map[string]Operator{}

var ErrNotMatched = errors.New("not matched")

func init() {
	// TODO: Add more operators.

	// register top level query operators matchers
	TopLevelQueryOperators["$and"] = matchAnd
	TopLevelQueryOperators["$nor"] = matchNor
	TopLevelQueryOperators["$or"] = matchOr

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
}

func Match(doc, query bsonkit.Doc) (bool, error) {
	// match document to query
	err := Process(&Context{
		TopLevel:   TopLevelQueryOperators,
		Expression: ExpressionQueryOperators,
	}, doc, *query, true)
	if err == ErrNotMatched {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func matchAnd(ctx *Context, doc bsonkit.Doc, _, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("$and: expected list")
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("$and: empty list")
	}

	// match all expressions
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("$and: expected list of documents")
		}

		// match document
		err := Process(ctx, doc, query, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func matchNor(ctx *Context, doc bsonkit.Doc, _, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("$nor: expected list")
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("$and: empty list")
	}

	// match first item
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("$nor: expected list of documents")
		}

		// match document
		err := Process(ctx, doc, query, false)
		if err == ErrNotMatched {
			return nil
		} else if err != nil {
			return err
		}

		return ErrNotMatched
	}

	return nil
}

func matchOr(ctx *Context, doc bsonkit.Doc, _, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("$or: expected list")
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("$and: empty list")
	}

	// match first item
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("$or: expected list of documents")
		}

		// match document
		err := Process(ctx, doc, query, false)
		if err != nil {
			return err
		} else {
			return nil
		}
	}

	return ErrNotMatched
}

func matchComp(ctx *Context, doc bsonkit.Doc, op, path string, v interface{}) error {
	// get field value
	field := bsonkit.Get(doc, path)

	// check types (type bracketing)
	if bsonkit.Inspect(field) != bsonkit.Inspect(v) {
		return ErrNotMatched
	}

	// compare field with value
	res := bsonkit.Compare(field, v)

	// handle special array field equality
	if array, ok := field.(bson.A); ok && op == "$eq" && res != 0 {
		for _, item := range array {
			if bsonkit.Compare(item, v) == 0 {
				return nil
			}
		}
	}

	// check operator
	var ok bool
	switch op {
	case "", "$eq":
		ok = res == 0
	case "$gt":
		ok = res > 0
	case "$gte":
		ok = res >= 0
	case "$lt":
		ok = res < 0
	case "$lte":
		ok = res <= 0
	case "$ne":
		ok = res != 0
	default:
		return fmt.Errorf("unkown comparison operator %q", op)
	}
	if !ok {
		return ErrNotMatched
	}

	return nil
}

func matchNot(ctx *Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// coerce item
	query, ok := v.(bson.D)
	if !ok {
		return fmt.Errorf("$not: expected document")
	}

	// check document
	if len(query) == 0 {
		return fmt.Errorf("$not: empty document")
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

func matchIn(ctx *Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("$in: expected list")
	}

	// get field value
	field := bsonkit.Get(doc, path)

	// check if field is in list
	for _, item := range list {
		if bsonkit.Compare(field, item) == 0 {
			return nil
		}
	}

	// check array elements
	if array, ok := field.(bson.A); ok {
		for _, entry := range array {
			for _, item := range list {
				if bsonkit.Compare(entry, item) == 0 {
					return nil
				}
			}
		}
	}

	// TODO: Support regular expressions.

	return ErrNotMatched
}

func matchNin(ctx *Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("$nin: expected list")
	}

	// get field value
	field := bsonkit.Get(doc, path)

	// check if field is not in list
	for _, item := range list {
		if bsonkit.Compare(field, item) == 0 {
			return ErrNotMatched
		}
	}

	// check array elements
	if array, ok := field.(bson.A); ok {
		for _, entry := range array {
			for _, item := range list {
				if bsonkit.Compare(entry, item) == 0 {
					return ErrNotMatched
				}
			}
		}
	}

	return nil
}

func matchExists(ctx *Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get boolean
	exists, ok := v.(bool)
	if !ok {
		return fmt.Errorf("$exists: expected boolean")
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
