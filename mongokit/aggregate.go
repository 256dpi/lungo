package mongokit

import "github.com/256dpi/lungo/bsonkit"

type Stage func(bsonkit.List) (bsonkit.List, error)

func Aggregate(list, stages bsonkit.List) (bsonkit.List, error)  {
	return list, nil
}
