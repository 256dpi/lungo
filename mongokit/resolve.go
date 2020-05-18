package mongokit

import (
	"fmt"
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
	// split path
	head, operator, tail := SplitDynamicPath(path)

	// immediately yield path if it does not include positional operators
	if operator == bsonkit.PathEnd {
		return callback(head)
	}

	// return error if path begins with a positional operator
	if head == bsonkit.PathEnd {
		return fmt.Errorf("unsupported root positional operator %q", operator)
	}

	// get array
	array, ok := bsonkit.Get(&doc, head).(bson.A)
	if !ok {
		return fmt.Errorf("expected array at %q to match against positional operator", head)
	}

	// check implicit positional operator "$"
	if operator == "$" {
		return fmt.Errorf("the implicit positional operator is not yet supported")
	}

	// check operator
	if !strings.HasPrefix(operator, "$[") || !strings.HasSuffix(operator, "]") {
		return fmt.Errorf("unknown positional operator %q", operator)
	}

	// get identifier
	identifier := operator[2 : len(operator)-1]

	// handle "all" positional operator "$[]"
	if identifier == "" {
		// prepare builder
		builder := bsonkit.NewPathBuilder(len(head) + 22 + len(tail))

		// add head
		fixed := builder.AddSegment(head)

		// construct path for all array elements
		for i := range array {
			// truncate builder
			builder.Truncate(fixed)

			// append index
			builder.AddIndex(i)

			// append tail if available
			if tail != bsonkit.PathEnd {
				builder.AddSegment(tail)
			}

			// resolve path
			err := resolve(builder.String(), query, doc, arrayFilters, callback)
			if err != nil {
				return err
			}
		}

		return nil
	}

	// prepare builder
	builder := bsonkit.NewPathBuilder(len(head) + 22 + len(tail))

	// add head
	fixed := builder.AddSegment(head)

	// handle identified positional operator "$[<identifier>]"
	for i, item := range array {
		// match item against provided array filters
		matched := false
		for _, filter := range arrayFilters {
			// match item
			ok, err := Match(&bson.D{
				bson.E{Key: identifier, Value: item},
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

		// truncate builder
		builder.Truncate(fixed)

		// append index
		builder.AddIndex(i)

		// append tail if available
		if tail != bsonkit.PathEnd {
			builder.AddSegment(tail)
		}

		// resolve path
		err := resolve(builder.String(), query, doc, arrayFilters, callback)
		if err != nil {
			return err
		}
	}

	return nil
}
