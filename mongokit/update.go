package mongokit

import "github.com/256dpi/lungo/bsonkit"

// Update will apply a MongoDB update document to a list of documents.
func Update(list bsonkit.List, update bsonkit.Doc, upsert bool) error {
	// apply update to all documents
	for _, item := range list {
		err := Apply(item, update, upsert)
		if err != nil {
			return err
		}
	}

	return nil
}
