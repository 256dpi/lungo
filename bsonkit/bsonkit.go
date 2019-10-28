package bsonkit

import "go.mongodb.org/mongo-driver/bson"

// Doc is a full document that may contain fields, arrays and embedded documents.
// The pointer form is chosen to identify the document uniquely (pointer address).
type Doc = *bson.D

// List is consecutive list of documents.
type List = []Doc
