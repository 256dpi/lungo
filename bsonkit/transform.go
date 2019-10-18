package bsonkit

import "go.mongodb.org/mongo-driver/bson"

func Transform(v interface{}) (Doc, error) {
	// TODO: This approach is slow, we should do it directly in memory if possible.

	// marshal to bytes
	bytes, err := bson.Marshal(v)
	if err != nil {
		return nil, err
	}

	// unmarshal bytes
	var doc bson.D
	err = bson.Unmarshal(bytes, &doc)
	if err != nil {
		return nil, err
	}

	return &doc, nil
}
