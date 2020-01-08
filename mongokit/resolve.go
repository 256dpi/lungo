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
	return resolve("", path, query, *doc, arrayFilters, callback)
}

func resolve(prevPath string, path string, query bsonkit.Doc, doc interface{}, arrayFilters bsonkit.List, callback func(path string) error) error {
	// check if path is ended
	if path == pathEnd || len(path) == 0 {
		return callback(prevPath)
	}
	// check if path includes positional operators
	if !strings.ContainsRune(path, '$') {
		//Build pathUpToNow
		if prevPath != "" {
			return callback(prevPath + "." + path)
		} else {
			return callback(path)
		}
	}
	//Get the parts
	staticPath, dynamicPart := dividePathStaticDynamicPart(path)
	operator := pathKey(dynamicPart)
	nextPath := pathShorten(dynamicPart)
	staticPathUpToNow := staticPath
	if prevPath != "" {
		staticPathUpToNow = prevPath + "." + staticPathUpToNow
	}

	switch doc := doc.(type) {
	case bson.D:
		value := bsonkit.Get(&doc, staticPath)

		//TODO: implement the $ operator
		if strings.HasPrefix(operator, "$[") && strings.HasSuffix(operator, "]") { //$[], $[<identifier>]
			//Extract the identifier operand
			identifier := operator[2 : len(operator)-1]
			if identifier == "" { //$[]
				if arr, ok := value.(bson.A); ok {
					for i, v := range arr {
						if err := resolve(staticPathUpToNow+"."+strconv.Itoa(i), nextPath, query, v, arrayFilters, callback); err != nil {
							return err
						}
					}
				} else {
					return fmt.Errorf("The value pointed in the path %q isn't a array", staticPathUpToNow)
				}
			} else { // $[<identifier>]
				//TODO: implement array filters
			}
		}
	default:
		return fmt.Errorf("The value pointed in the path %q isn't a *bson.D", prevPath)
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
