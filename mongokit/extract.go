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
	}, doc, *query, "", true)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func extractAnd(ctx *Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get array
	array, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected array", name)
	}

	// check array
	if len(array) == 0 {
		return fmt.Errorf("%s: empty array", name)
	}

	// extract all expressions
	for _, item := range array {
		// coerce item
		query, ok := item.(bson.D)
		if !ok {
			return fmt.Errorf("%s: expected array of documents", name)
		}

		// extract document
		err := Process(ctx, doc, query, "", false)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractOr(ctx *Context, doc bsonkit.Doc, name, _ string, v interface{}) error {
	// get array
	array, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected array", name)
	}

	// check array
	if len(array) == 0 {
		return fmt.Errorf("%s: empty array", name)
	}

	// ignore longer arrays
	if len(array) > 1 {
		return nil
	}

	// coerce first item
	query, ok := array[0].(bson.D)
	if !ok {
		return fmt.Errorf("%s: expected array of documents", name)
	}

	// extract document
	err := Process(ctx, doc, query, "", false)
	if err != nil {
		return err
	}

	return nil
}

func extractEq(_ *Context, doc bsonkit.Doc, _, path string, v interface{}) error {
	return bsonkit.Put(doc, path, v, false)
}

func extractIn(_ *Context, doc bsonkit.Doc, name, path string, v interface{}) error {
	// get array
	array, ok := v.(bson.A)
	if !ok {
		return fmt.Errorf("%s: expected array", name)
	}

	// check array
	if len(array) == 1 {
		return bsonkit.Put(doc, path, array[0], false)
	}

	return nil
}
