package mongokit

import (
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// TODO: Implement implicit positional operators.

// Resolve will resolve all positional operators in the provided path using the
// query, document and array filters. For each match it will call the callback
// with the generated absolute path.
func Resolve(path string, query, doc bsonkit.Doc, arrayFilters bsonkit.List, callback func(path string) error) error {
	return resolve(path, query, *doc, arrayFilters, callback)
}

func resolve(path string, query bsonkit.Doc, doc bson.D, arrayFilters bsonkit.List, callback func(path string) error) error {
	// immediately yield path if it does not include positional operators
	if !strings.ContainsRune(path, '$') {
		return callback(path)
	}

	// Get the parts
	staticPart, dynamicPart := dividePathStaticDynamicPart(path)
	operator := bsonkit.PathSegment(dynamicPart)
	nextPath := bsonkit.PathReduce(dynamicPart)

	value := bsonkit.Get(&doc, staticPart)

	// get array
	array, ok := value.(bson.A)
	if !ok {
		return fmt.Errorf("the value pointed in the path %q isn't a array", staticPart)
	}

	// check implicit positional operator
	if operator == "$" {
		return fmt.Errorf("implicit positional operator not supported")
	}

	// check operator
	if !strings.HasPrefix(operator, "$[") || !strings.HasSuffix(operator, "]") {
		return fmt.Errorf("the positional operator %q is not supported", operator)
	}

	// get identifier
	identifier := operator[2 : len(operator)-1]

	// handle "all" positional operator: $[]
	if identifier == "" {
		// construct path for all array elements
		for i := range array {
			// construct static path
			currentPath := staticPart + "." + strconv.Itoa(i)

			// append next path if available
			if nextPath != bsonkit.PathEnd && nextPath != "" {
				currentPath += "." + nextPath
			}

			// resolve path
			err := resolve(currentPath, query, doc, arrayFilters, callback)
			if err != nil {
				return err
			}
		}

		return nil
	}

	// handle identified positional operator: $[<identifier>]
	for i, val := range array {
		// check if the current item match don't the arrayFilters with identifier name
		matched := false
		for _, filter := range arrayFilters {
			// TODO: Add filter checking!
			ok, err := Match(&bson.D{
				bson.E{Key: identifier, Value: val},
			}, filter)
			if err != nil {
				return err
			}

			// check if matched
			if ok {
				matched = true
				break
			}
		}

		// continue if not matched
		if !matched {
			continue
		}

		// construct static path
		currentPath := staticPart + "." + strconv.Itoa(i)

		// append next path if available
		if nextPath != bsonkit.PathEnd && nextPath != "" {
			currentPath += "." + nextPath
		}

		// resolve path
		err := resolve(currentPath, query, doc, arrayFilters, callback)
		if err != nil {
			return err
		}
	}

	return nil
}

func dividePathStaticDynamicPart(remainingPath string) (string, string) {
	if strings.HasPrefix(remainingPath, "$") || remainingPath == bsonkit.PathEnd || remainingPath == "" {
		return bsonkit.PathEnd, remainingPath
	}

	pathKey := bsonkit.PathSegment(remainingPath)
	subStaticPart, subDynamicPart := dividePathStaticDynamicPart(bsonkit.PathReduce(remainingPath))

	if subStaticPart == bsonkit.PathEnd {
		return pathKey, subDynamicPart
	}

	return pathKey + "." + subStaticPart, subDynamicPart
}
