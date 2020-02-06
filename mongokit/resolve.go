package mongokit

import (
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

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

	// TODO: implement the $ operator
	if strings.HasPrefix(operator, "$[") && strings.HasSuffix(operator, "]") { // $[], $[<identifier>]
		if arr, ok := value.(bson.A); ok {
			// Extract the identifier operand
			identifier := operator[2 : len(operator)-1]
			if identifier == "" { // $[]
				for i := range arr {
					currentPath := staticPart + "." + strconv.Itoa(i)
					if nextPath != bsonkit.PathEnd && nextPath != "" {
						currentPath += "." + nextPath
					}
					if err := resolve(currentPath, query, doc, arrayFilters, callback); err != nil {
						return err
					}
				}
			} else { // $[<identifier>]
				// TODO: implement array filters<identifier>
				for i, val := range arr {
					currentPath := staticPart + "." + strconv.Itoa(i)
					matched := false
					// Check if the current item match don't the arrayFilters with identifier name
					for _, filter := range arrayFilters {
						// TODO: Add filter checking!
						if val, err := Match(&bson.D{
							bson.E{Key: identifier, Value: val},
						}, filter); err != nil {
							return err
						} else if val {
							matched = true
							break
						}
					}
					if !matched {
						continue
					}
					if nextPath != bsonkit.PathEnd && nextPath != "" {
						currentPath += "." + nextPath
					}
					if err := resolve(currentPath, query, doc, arrayFilters, callback); err != nil {
						return err
					}
				}
			}
		} else {
			return fmt.Errorf("the value pointed in the path %q isn't a array", staticPart)
		}
	} else {
		return fmt.Errorf("the operatpr %q is not supported", operator)
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
