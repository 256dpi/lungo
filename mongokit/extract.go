package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// https://github.com/mongodb/mongo/blob/master/src/mongo/db/extracter/expression_leaf.cpp

type ExtractOperator func(bsonkit.Doc, string, interface{}) error

var TopLevelExtractOperators = map[string]ExtractOperator{}
var ExpressionExtractOperators = map[string]ExtractOperator{}

func init() {
	// register top level extractors
	TopLevelExtractOperators["$and"] = extractAnd
	TopLevelExtractOperators["$or"] = extractOr

	// register expression extractors
	ExpressionExtractOperators[""] = extractEq
	ExpressionExtractOperators["$eq"] = extractEq
	ExpressionExtractOperators["$in"] = extractIn
}

func Extract(query bsonkit.Doc) (bsonkit.Doc, error) {
	// prepare doc
	doc := &bson.D{}

	// extract query
	err := extractAll(doc, *query, true)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func extractAll(doc bsonkit.Doc, query bson.D, root bool) error {
	// extract all expressions
	for _, exp := range query {
		err := extractExpression(doc, "", exp, root)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractExpression(doc bsonkit.Doc, prefix string, pair bson.E, root bool) error {
	// check for top level operators which may appear together with field
	// expressions in the query document
	if len(pair.Key) > 0 && pair.Key[0] == '$' {
		// lookup top level operator
		var operator ExtractOperator
		if root {
			operator = TopLevelExtractOperators[pair.Key]
		} else {
			operator = ExpressionExtractOperators[pair.Key]
		}
		if operator == nil {
			return fmt.Errorf("extract: unknown top level operator %q", pair.Key)
		}

		// call operator
		return operator(doc, prefix, pair.Value)
	}

	// get path
	path := pair.Key
	if prefix != "" {
		path = prefix + "." + path
	}

	// check for field expressions with a document which may contain either
	// only expression operators or only simple equality conditions
	if exps, ok := pair.Value.(bson.D); ok {
		// extract all expressions if found
		for i, exp := range exps {
			// break and leave document as a simple equality condition if the
			// first key does not look like an operator
			if i == 0 && (len(exp.Key) == 0 || exp.Key[0] != '$') {
				break
			}

			// check operator validity
			if len(exp.Key) == 0 || exp.Key[0] != '$' {
				return fmt.Errorf("extract: expected operator, got %q", exp.Key)
			}

			// lookup operator
			operator := ExpressionExtractOperators[exp.Key]
			if operator == nil {
				return fmt.Errorf("extract: unknown operator %q", exp.Key)
			}

			// call operator
			err := operator(doc, path, exp.Value)
			if err != nil {
				return err
			}

			// return success if last one
			if i == len(exps)-1 {
				return nil
			}
		}
	}

	// handle pair as a simple equality condition

	// get the equality query operator
	operator := ExpressionExtractOperators[""]
	if operator == nil {
		return fmt.Errorf("extract: missing default equality operator")
	}

	// call equality operator
	err := operator(doc, path, pair.Value)

	return err
}

func extractAnd(doc bsonkit.Doc, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("extract: $and: expected list")
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("extract: $and: empty list")
	}

	// extract all expressions
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("extract: $and: expected list of documents")
		}

		// extract document
		err := extractAll(doc, query, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractOr(doc bsonkit.Doc, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("extract: $or: expected list")
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("extract: $or: empty list")
	}

	// check list
	if len(list) > 1 {
		return nil
	}

	// extract first item
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("extract: $or: expected list of documents")
		}

		// extract document
		err := extractAll(doc, query, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractEq(doc bsonkit.Doc, path string, v interface{}) error {
	return bsonkit.Set(doc, path, v, false)
}

func extractIn(doc bsonkit.Doc, path string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("extract: $in: expected list")
	}

	// check list
	if len(list) == 1 {
		return bsonkit.Set(doc, path, list[0], false)
	}

	return nil
}
