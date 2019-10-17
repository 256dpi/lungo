package bsonkit

import "go.mongodb.org/mongo-driver/bson"

func Transform(doc interface{}) (bson.D, error) {
	// TODO: This approach is slow, we should do it directly in memory if possible.

	// marshal to bytes
	bytes, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}

	// unmarshal bytes
	var d bson.D
	err = bson.Unmarshal(bytes, &d)
	if err != nil {
		return nil, err
	}

	return d, nil
}
