package bsonkit

import (
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

var tsSeconds uint32
var tsCounter uint32
var tsMutex sync.Mutex

// Now will generate a locally monotonic timestamp.
func Now() primitive.Timestamp {
	// acquire mutex
	tsMutex.Lock()
	defer tsMutex.Unlock()

	// get current time
	now := uint32(time.Now().Unix())

	// check if reset is needed
	if tsSeconds < now {
		tsSeconds = now
		tsCounter = 0
	}

	// increment counter
	tsCounter++

	return primitive.Timestamp{
		T: tsSeconds,
		I: tsCounter,
	}
}
