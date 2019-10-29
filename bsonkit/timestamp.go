package bsonkit

import (
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

var tsSeconds uint32
var tsCounter uint32
var tsTest bool
var tsMutex sync.Mutex

// Now will generate a locally monotonic timestamp.
func Now() primitive.Timestamp {
	// acquire mutex
	tsMutex.Lock()
	defer tsMutex.Unlock()

	// update if not in test
	if !tsTest {
		// get current time
		now := uint32(time.Now().Unix())

		// check if reset is needed
		if tsSeconds != now {
			tsSeconds = now
			tsCounter = 1
		}
	}

	// increment counter
	tsCounter++

	return primitive.Timestamp{
		T: tsSeconds,
		I: tsCounter,
	}
}

// ResetCounter will disable the time portion of generate timestamps and reset
// the ordinal counter to zero. This function should only be called in tests
// to generate predictable timestamps.
func ResetCounter() {
	// acquire mutex
	tsMutex.Lock()
	defer tsMutex.Unlock()

	// reset
	tsTest = true
	tsSeconds = 0
	tsCounter = 0
}
