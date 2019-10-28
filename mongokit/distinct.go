package mongokit

import (
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// Distinct will perform a MongoDB distinct value search on the list of documents
// and return an array with the results.
func Distinct(list bsonkit.List, path string) bson.A {
	return bsonkit.Collect(list, path, true, true, true, true)
}
