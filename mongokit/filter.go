package mongokit

import "go.mongodb.org/mongo-driver/bson"

func Filter(list []bson.D, query bson.D, limit int) ([]bson.D, error) {
	// filter list based on query
	var ret []bson.D
	for _, item := range list {
		// match item
		res, err := Match(item, query)
		if err != nil {
			return nil, err
		} else if res {
			ret = append(ret, item)
		}

		// check limit
		if limit > 0 && len(ret) >= limit {
			return ret, nil
		}
	}

	return ret, nil
}
