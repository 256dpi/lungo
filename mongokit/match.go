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
	ExpressionQueryOperators["$all"] = matchAll
	ExpressionQueryOperators["$size"] = matchSize
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
	return matchUnwind(doc, path, false, func(field interface{}) error {
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
	return matchUnwind(doc, path, false, func(field interface{}) error {
		// get array
		list, ok := v.(bson.A)
		if !ok {
			return fmt.Errorf("%s: expected list", name)
		}

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
	field := bsonkit.Get(doc, path, false)
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

func matchAll(_ *Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchUnwind(doc, path, false, func(field interface{}) error {
		// get array
		list, ok := v.(bson.A)
		if !ok {
			return fmt.Errorf("%s: expected list", name)
		}

		// check array
		if len(list) == 0 {
			return ErrNotMatched
		}

		// check if array contains list
		if arr, ok := field.(bson.A); ok {
			matches := true
			for _, value := range list {
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

		// check if field is in list
		for _, item := range list {
			if bsonkit.Compare(field, item) != 0 {
				return ErrNotMatched
			}
		}

		return nil
	})
}

func matchSize(_ *Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	return matchUnwind(doc, path, false, func(field interface{}) error {
		// check value
		if bsonkit.Inspect(v) != bsonkit.Number {
			return fmt.Errorf("%s: expected number", name)
		}

		// compare length if array
		list, ok := field.(bson.A)
		if ok {
			if bsonkit.Compare(int64(len(list)), v) == 0 {
				return nil
			}
		}

		return ErrNotMatched
	})
}

func matchUnwind(doc bsonkit.Doc, path string, collect bool, op func(interface{}) error) error {
	// get value
	value := bsonkit.Get(doc, path, collect)
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

	return op(value)
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
