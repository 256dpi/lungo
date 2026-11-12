package lungo

import "go.mongodb.org/mongo-driver/v2/mongo"

// GridFS symbols renamed or moved in mongo-driver v2; keep short aliases for lungo internals/tests.

const gridfsDefaultChunkSize = mongo.DefaultGridFSChunkSize

// DefaultChunkSize matches the official driver's default GridFS chunk size.
const DefaultChunkSize = gridfsDefaultChunkSize

// UploadBufferSize matches the official driver's gridfs upload batch size (16 MiB).
const UploadBufferSize = 16 * 1024 * 1024
