package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// https://github.com/mongodb/mongo/blob/master/src/mongo/db/extracter/expression_leaf.cpp

var TopLevelExtractOperators = map[string]Operator{}
var ExpressionExtractOperators = map[string]Operator{}

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

	// match document to query
	err := Process(&Context{
		TopLevel:   TopLevelExtractOperators,
		Expression: ExpressionExtractOperators,
	}, doc, *query, true)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func extractAnd(ctx *Context, doc bsonkit.Doc, _, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("$and: expected list")
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("$and: empty list")
	}

	// extract all expressions
	for _, item := range list {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("$and: expected list of documents")
		}

		// extract document
		err := Process(ctx, doc, query, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractOr(ctx *Context, doc bsonkit.Doc, _, _ string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("$or: expected list")
	}

	// check list
	if len(list) == 0 {
		return fmt.Errorf("$or: empty list")
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
			return fmt.Errorf("$or: expected list of documents")
		}

		// extract document
		err := Process(ctx, doc, query, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractEq(ctx *Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	return bsonkit.Put(doc, path, v, false)
}

func extractIn(ctx *Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("$in: expected list")
	}

	// check list
	if len(list) == 1 {
		return bsonkit.Put(doc, path, list[0], false)
	}

	return nil
}
