package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// https://github.com/mongodb/mongo/blob/master/src/mongo/db/matcher/expression_leaf.cpp

type Operator func(bson.D, string, interface{}) (bool, error)

var TopLevelQueryOperators = map[string]Operator{}
var ExpressionQueryOperators = map[string]Operator{}

func init() {
	// TODO: Add more operators.

	// register top level query operators matchers
	TopLevelQueryOperators["$and"] = matchAnd
	TopLevelQueryOperators["$not"] = matchNot
	TopLevelQueryOperators["$nor"] = matchNor
	TopLevelQueryOperators["$or"] = matchOr

	// register expression query operators
	ExpressionQueryOperators["$and"] = matchAnd
	ExpressionQueryOperators["$not"] = matchNot
	ExpressionQueryOperators["$nor"] = matchNor
	ExpressionQueryOperators["$or"] = matchOr
	ExpressionQueryOperators["$eq"] = matchComp("$eq")
	ExpressionQueryOperators["$gt"] = matchComp("$gt")
	ExpressionQueryOperators["$lt"] = matchComp("$lt")
	ExpressionQueryOperators["$gte"] = matchComp("$gte")
	ExpressionQueryOperators["$lte"] = matchComp("$lte")
	ExpressionQueryOperators["$ne"] = matchComp("$ne")
	ExpressionQueryOperators["$in"] = matchIn
	ExpressionQueryOperators["$nin"] = matchNin
	ExpressionQueryOperators["$exists"] = matchExists
}

func Match(doc, query bson.D) (bool, error) {
	// match all expressions (implicit and)
	for _, exp := range query {
		ok, err := matchQueryPair(doc, exp)
		if err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
	}

	return true, nil
}

func matchQueryPair(doc bson.D, pair bson.E) (bool, error) {
	// check for top level query operator which may appear together with field
	// expressions in the query filter document
	if len(pair.Key) > 0 && pair.Key[0] == '$' {
		// lookup top level operator
		operator := TopLevelQueryOperators[pair.Key]
		if operator == nil {
			return false, fmt.Errorf("match: unknown top level operator %q", pair.Key)
		}

		// call operator
		return operator(doc, "", pair.Value)
	}

	// check for field expressions with a document which may contain either
	// only expression query operators or only simple equality conditions
	if exps, ok := pair.Value.(bson.D); ok {
		// match all expressions if found (implicit and)
		for i, exp := range exps {
			// break and leave document as a simple equality condition if the
			// first key does not look like an operator
			if i == 0 && (len(exp.Key) == 0 || exp.Key[0] != '$') {
				break
			}

			// check operator validity
			if len(exp.Key) == 0 || exp.Key[0] != '$' {
				return false, fmt.Errorf("match: expected operator, got %q", exp.Key)
			}

			// lookup operator
			operator := ExpressionQueryOperators[exp.Key]
			if operator == nil {
				return false, fmt.Errorf("match: unknown operator %q", exp.Key)
			}

			// call matcher
			ok, err := operator(doc, pair.Key, exp.Value)
			if err != nil {
				return false, err
			} else if !ok {
				return false, nil
			}

			// return success if last one
			if i == len(exps)-1 {
				return true, nil
			}
		}
	}

	// handle pair as a simple equality condition

	// get the equality query operator
	operator := ExpressionQueryOperators["$eq"]
	if operator == nil {
		return false, fmt.Errorf("match: missing default equality operator")
	}

	// call equality query operator
	res, err := operator(doc, pair.Key, pair.Value)

	return res, err
}

func matchAnd(doc bson.D, _ string, v interface{}) (bool, error) {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return false, fmt.Errorf("match: $and: expected list")
	}

	// check list
	if len(list) == 0 {
		return false, fmt.Errorf("match: $and: empty list")
	}

	// match all expressions
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return false, fmt.Errorf("match: $and: expected list of documents")
		}

		// match document
		ok, err := Match(doc, query)
		if err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
	}

	return true, nil
}

func matchNot(doc bson.D, _ string, v interface{}) (bool, error) {
	// coerce item
	query, ok := v.(bson.D)
	if !ok {
		return false, fmt.Errorf("match: $not: expected document")
	}

	// match document
	ok, err := Match(doc, query)
	if err != nil {
		return false, err
	}

	return !ok, nil
}

func matchNor(doc bson.D, _ string, v interface{}) (bool, error) {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return false, fmt.Errorf("match: $nor: expected list")
	}

	// check list
	if len(list) == 0 {
		return false, fmt.Errorf("match: $and: empty list")
	}

	// match first item
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return false, fmt.Errorf("match: $nor: expected list of documents")
		}

		// match document
		ok, err := Match(doc, query)
		if err != nil {
			return false, err
		} else if ok {
			return false, nil
		}
	}

	return true, nil
}

func matchOr(doc bson.D, _ string, v interface{}) (bool, error) {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return false, fmt.Errorf("match: $or: expected list")
	}

	// check list
	if len(list) == 0 {
		return false, fmt.Errorf("match: $and: empty list")
	}

	// match first item
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return false, fmt.Errorf("match: $or: expected list of documents")
		}

		// match document
		ok, err := Match(doc, query)
		if err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}

	return false, nil
}

func matchComp(op string) Operator {
	return func(doc bson.D, path string, v interface{}) (bool, error) {
		// get field value
		field := bsonkit.Get(doc, path)

		// compare field with value
		res := bsonkit.Compare(field, v)

		// check operator
		switch op {
		case "$eq":
			return res == 0, nil
		case "$gt":
			return res > 0, nil
		case "$gte":
			return res >= 0, nil
		case "$lt":
			return res < 0, nil
		case "$lte":
			return res <= 0, nil
		case "$ne":
			return res != 0, nil
		default:
			return false, fmt.Errorf("match: unkown comparison operator %q", op)
		}
	}
}

func matchIn(doc bson.D, path string, v interface{}) (bool, error) {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return false, fmt.Errorf("match: $in: expected list")
	}

	// get field value
	field := bsonkit.Get(doc, path)

	// check if field is in list
	for _, item := range list {
		res := bsonkit.Compare(field, item)
		if res == 0 {
			return true, nil
		}
	}

	return false, nil
}

func matchNin(doc bson.D, path string, v interface{}) (bool, error) {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return false, fmt.Errorf("match: $nin: expected list")
	}

	// get field value
	field := bsonkit.Get(doc, path)

	// check if field is not in list
	for _, item := range list {
		res := bsonkit.Compare(field, item)
		if res == 0 {
			return false, nil
		}
	}

	return true, nil
}

func matchExists(doc bson.D, path string, v interface{}) (bool, error) {
	// get boolean
	exists, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("match: $exists: expected boolean")
	}

	// get field value
	field := bsonkit.Get(doc, path)
	if exists {
		return field != bsonkit.Missing, nil
	}

	return field == bsonkit.Missing, nil
}
