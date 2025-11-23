package mongokit

import (
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/256dpi/lungo/bsonkit"
)

// Distinct will perform a MongoDB distinct value search on the list of documents
// and return a raw BSON array with the results.
func Distinct(list bsonkit.List, path string) bson.RawArray {
	return marshalArray(bsonkit.Collect(list, path, true, true, true, true))
}

// marshalArray will marshal the provided bson.A into a bson.RawArray by wrapping
// it in a temporary document. This avoids creating a top-level BSON array,
// which is not allowed by the BSON specification.
func marshalArray(arr bson.A) bson.RawArray {
	doc, err := bson.Marshal(bson.M{"a": arr})
	if err != nil {
		panic(err)
	}

	rawDoc := bson.Raw(doc)
	rawVal := rawDoc.Lookup("a")

	return rawVal.Array()
}
