package lungo

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/256dpi/lungo/bsonkit"
)

type SingleResult struct {
	doc bsonkit.Doc
	err error
}

func (r *SingleResult) Decode(out interface{}) error {
	// check error
	if r.err != nil {
		return r.err
	}

	// check document
	if r.doc == nil {
		return mongo.ErrNoDocuments
	}

	// decode document
	return bsonkit.Decode(r.doc, out)
}

func (r *SingleResult) DecodeBytes() (bson.Raw, error) {
	// check error
	if r.err != nil {
		return nil, r.err
	}

	// check document
	if r.doc == nil {
		return nil, mongo.ErrNoDocuments
	}

	// marshal document
	return bson.Marshal(r.doc)
}

func (r *SingleResult) Err() error {
	// check error
	if r.err != nil {
		return r.err
	}

	// check document
	if r.doc == nil {
		return mongo.ErrNoDocuments
	}

	return nil
}
