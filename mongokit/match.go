package mongokit

import (
	"errors"
	"fmt"
	"strconv"

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
	TopLevelQueryOperators["$or"] = matchOr
	TopLevelQueryOperators["$nor"] = matchNor

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

func matchAnd(ctx *Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected list", name)
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("%s: empty list", name)
	}

	// match all expressions
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected list of documents", name)
		}

		// match document
		err := Process(ctx, doc, query, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func matchOr(ctx *Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected list", name)
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("%s: empty list", name)
	}

	// match first item
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected list of documents", name)
		}

		// match document
		err := Process(ctx, doc, query, false)
		if err == ErrNotMatched {
			continue
		} else if err != nil {
			return err
		}

		return nil
	}

	return ErrNotMatched
}

func matchNor(ctx *Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchNegate(func() error {
		return matchOr(ctx, doc, name, path, v)
	})
}

func matchComp(_ *Context, doc bsonkit.Doc, op, path string, v interface{}) error {
	return matchUnwind(doc, path, func(path string) error {
		// get field value
		field := bsonkit.Get(doc, path)

		// check types (type bracketing)
		if bsonkit.Inspect(field) != bsonkit.Inspect(v) {
			return ErrNotMatched
		}

		// compare field with value
		res := bsonkit.Compare(field, v)

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
	})
}

func matchNot(ctx *Context, doc bsonkit.Doc, name, path string, v interface{}) error {
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

func matchIn(_ *Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchUnwind(doc, path, func(path string) error {
		// get array
		list, ok := v.(bson.A)
		if !ok {
			return fmt.Errorf("%s: expected list", name)
		}

		// get field value
		field := bsonkit.Get(doc, path)

		// check if field is in list
		for _, item := range list {
			if bsonkit.Compare(field, item) == 0 {
				return nil
			}
		}

		// TODO: Support regular expressions.

		return ErrNotMatched
	})
}

func matchNin(ctx *Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchNegate(func() error {
		return matchIn(ctx, doc, name, path, v)
	})
}

func matchExists(_ *Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// get boolean
	exists, ok := v.(bool)
	if !ok {
		return fmt.Errorf("%s: expected boolean", name)
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

func matchUnwind(doc bsonkit.Doc, path string, op func(string) error) error {
	// get value
	value := bsonkit.Get(doc, path)
	if arr, ok := value.(bson.A); ok {
		for i := range arr {
			err := op(path + "." + strconv.Itoa(i))
			if err == ErrNotMatched {
				continue
			} else if err != nil {
				return err
			}

			return nil
		}
	}

	return op(path)
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
