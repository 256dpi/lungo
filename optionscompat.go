package lungo

import "go.mongodb.org/mongo-driver/v2/mongo/options"

// Driver v2 split one/options types; keep v1 names on lungo interfaces.
type (
	DeleteOptions = options.DeleteOneOptions
	UpdateOptions = options.UpdateOneOptions
	UploadOptions = options.GridFSUploadOptions
	NameOptions   = options.GridFSNameOptions
)
