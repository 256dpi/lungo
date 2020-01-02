package mongokit

import "github.com/256dpi/lungo/bsonkit"

// Update will apply a MongoDB update document to a list of documents.
func Update(list bsonkit.List, update bsonkit.Doc, upsert bool, arrayFilters bsonkit.List) ([]*Changes, error) {
	// prepare result
	result := make([]*Changes, 0, len(list))

	// apply update to all documents and collect changes
	for _, item := range list {
		changes, err := Apply(item, update, upsert, arrayFilters)
		if err != nil {
			return nil, err
		}
		result = append(result, changes)
	}

	return result, nil
}
