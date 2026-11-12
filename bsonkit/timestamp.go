package bsonkit

import (
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var tsSeconds uint32
var tsCounter uint32
var tsMutex sync.Mutex

// Now will generate a locally monotonic timestamp.
func Now() bson.Timestamp {
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

	// increment counter, advancing T on overflow to preserve monotonicity
	if tsCounter == ^uint32(0) {
		tsSeconds++
		tsCounter = 0
	}
	tsCounter++

	return bson.Timestamp{
		T: tsSeconds,
		I: tsCounter,
	}
}
