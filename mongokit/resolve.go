package mongokit

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/256dpi/lungo/bsonkit"
	"go.mongodb.org/mongo-driver/bson"
)

// Resolve will resolve all positional operators in the provided path using the
// query, document and array filters. For each match it will call the callback
// with the generated absolute path.
func Resolve(path string, query, doc bsonkit.Doc, arrayFilters bsonkit.List, callback func(path string) error) error {
	return resolve(path, query, *doc, arrayFilters, callback)
}

func resolve(path string, query bsonkit.Doc, doc interface{}, arrayFilters bsonkit.List, callback func(path string) error) error {
	// check if path includes positional operators
	if !strings.ContainsRune(path, '$') {
		//Build pathUpToNow
		return callback(path)
	}
	//Get the parts
	staticPart, dynamicPart := dividePathStaticDynamicPart(path)
	operator := pathKey(dynamicPart)
	nextPath := pathShorten(dynamicPart)

	switch doc := doc.(type) {
	case bson.D:
		value := bsonkit.Get(&doc, staticPart)

		//TODO: implement the $ operator
		if strings.HasPrefix(operator, "$[") && strings.HasSuffix(operator, "]") { //$[], $[<identifier>]
			//Extract the identifier operand
			identifier := operator[2 : len(operator)-1]
			if identifier == "" { //$[]
				if arr, ok := value.(bson.A); ok {
					for i, _ := range arr {
						currentPath := staticPart + "." + strconv.Itoa(i)
						if nextPath != pathEnd && nextPath != "" {
							currentPath += "." + nextPath
						}
						if err := resolve(currentPath, query, doc, arrayFilters, callback); err != nil {
							return err
						}
					}
				} else {
					return fmt.Errorf("The value pointed in the path %q isn't a array", staticPart)
				}
			} else { // $[<identifier>]
				//TODO: implement array filters
			}
		} else {
			return fmt.Errorf("The operatpr %q is not supported", operator)
		}
	default:
		return fmt.Errorf("The value pointed in the path %q isn't a *bson.D", path)
	}
	return nil
}

var pathEnd = "\x00"

func pathShorten(path string) string {
	i := strings.IndexByte(path, '.')
	if i >= 0 {
		return path[i+1:]
	}

	return pathEnd
}

func pathKey(path string) string {
	i := strings.IndexByte(path, '.')
	if i >= 0 {
		return path[:i]
	}

	return path
}

func dividePathStaticDynamicPart(remainingPath string) (string, string) {
	if strings.HasPrefix(remainingPath, "$") || remainingPath == pathEnd || remainingPath == "" {
		return pathEnd, remainingPath
	}

	pathKey := pathKey(remainingPath)
	subStaticPart, subDynamicPart := dividePathStaticDynamicPart(pathShorten(remainingPath))

	if subStaticPart == pathEnd {
		return pathKey, subDynamicPart
	} else {
		return pathKey + "." + subStaticPart, subDynamicPart
	}
}
