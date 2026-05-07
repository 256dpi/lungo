package lungo

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/256dpi/lungo/bsonkit"
)

// ErrNoDocuments is returned by SingleResult if not document has been found.
// The value is the same as mongo.ErrNoDocuments and can be used interchangeably.
var ErrNoDocuments = mongo.ErrNoDocuments

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
		return ErrNoDocuments
	}

	// decode document
	return bsonkit.Decode(r.doc, out)
}

// Err implements the ISingleResult.Err method.
func (r *SingleResult) Err() error {
	// check error
	if r.err != nil {
		return r.err
	}

	// check document
	if r.doc == nil {
		return ErrNoDocuments
	}

	return nil
}

// Raw implements the ISingleResult.Raw method.
func (r *SingleResult) Raw() (bson.Raw, error) {
	// check error
	if r.err != nil {
		return nil, r.err
	}

	// check document
	if r.doc == nil {
		return nil, ErrNoDocuments
	}

	// marshal document
	return bson.Marshal(r.doc)
}
