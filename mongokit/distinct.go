package mongokit

import (
	"go.mongodb.org/mongo-driver/bson"

	"github.com/256dpi/lungo/bsonkit"
)

func Distinct(list bsonkit.List, path string) bson.A {
	return bsonkit.Collect(list, path, true, true, true, true)
}
