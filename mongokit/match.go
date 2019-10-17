package mongokit

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// https://github.com/mongodb/mongo/blob/master/src/mongo/db/matcher/expression_leaf.cpp

var Matchers = map[string]func(bson.D, string, interface{}) (bool, error){}

func init() {
	// TODO: Add more operators.

	// register logical matchers
	Matchers["$and"] = matchAnd
	Matchers["$not"] = matchNot
	Matchers["$nor"] = matchNor
	Matchers["$or"] = matchOr

	// register comparison matchers
	Matchers["$eq"] = matchComp("$eq")
	Matchers["$gt"] = matchComp("$gt")
	Matchers["$lt"] = matchComp("$lt")
	Matchers["$gte"] = matchComp("$gte")
	Matchers["$lte"] = matchComp("$lte")
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

// the query document may contain root matchers "{ $and: [], ...}", field matchers
// "{ field: { $exp: value }, ... }" and simple equality matchers "{ field: value }, ... }"

// expressions documents however may only contain expressions "$exp: value, ..."

func matchQueryPair(doc bson.D, pair bson.E) (bool, error) {
	// handle root matchers e.g. $and, $or, ...
	rootMatcher := Matchers[pair.Key]
	if rootMatcher != nil {
		return rootMatcher(doc, "", pair.Value)
	}

	// check document matcher "{ field: { $exp: value } }"
	if exps, ok := pair.Value.(bson.D); ok {
		// check if there are operators
		found := false
		for _, exp := range exps {
			if exp.Key[0] == '$' {
				found = true
			}
		}

		// match all expressions if found (implicit and)
		if found {
			for _, exp := range exps {
				// lookup matcher
				matcher := Matchers[exp.Key]
				if matcher == nil {
					return false, fmt.Errorf("match: unkown matcher %q", exp.Key)
				}

				// call matcher
				ok, err := matcher(doc, pair.Key, exp.Value)
				if err != nil {
					return false, err
				} else if !ok {
					return false, nil
				}
			}

			return true, nil
		}
	}

	// use default equality matcher "{ field: value } }"
	return Matchers["$eq"](doc, pair.Key, pair.Value)
}

func matchAnd(doc bson.D, _ string, v interface{}) (bool, error) {
	// get array
	list, ok := v.(bson.A)
	if !ok {
		return false, fmt.Errorf("match: $and: expected list")
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

func matchComp(typ string) func(bson.D, string, interface{}) (bool, error) {
	return func(doc bson.D, path string, v interface{}) (bool, error) {
		// get field value
		field := bsonkit.Get(doc, path)
		if field == bsonkit.Missing {
			field = nil
		}

		// compare field with value
		res, err := bsonkit.Compare(field, v)
		if err != nil {
			return false, err
		}

		// check type
		switch typ {
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
		}

		return false, fmt.Errorf("match: unreachable")
	}
}
