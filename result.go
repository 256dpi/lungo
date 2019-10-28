package lungo

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/256dpi/lungo/bsonkit"
)

var _ ISingleResult = &SingleResult{}

// SingleResult wraps a result to be mongo compatible.
type SingleResult struct {
	doc bsonkit.Doc
	err error
}

// Decode implements the ISingleResult.Decode method.
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

// DecodeBytes implements the ISingleResult.DecodeBytes method.
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

// Err implements the ISingleResult.Err method.
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
