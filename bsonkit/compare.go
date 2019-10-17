package bsonkit

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// https://github.com/mongodb/mongo/blob/master/src/mongo/bson/bsonelement.cpp
// https://github.com/mongodb/mongo/blob/master/src/mongo/bson/bsonobj.cpp

func Compare(lv, rv interface{}) (int, error) {
	// inspect left value
	lt, err := Inspect(lv)
	if err != nil {
		return 0, err
	}

	// inspect right value
	rt, err := Inspect(rv)
	if err != nil {
		return 0, err
	}

	// check type equality
	if lt > rt {
		return 1, nil
	} else if lt < rt {
		return -1, nil
	}

	// check value equality
	switch lt {
	case Null:
		return 0, nil
	case Number:
		return compareNumbers(lv, rv)
	case String:
		return compareStrings(lv, rv)
	case Object:
		return compareObjects(lv, rv)
	case Array:
		return compareArrays(lv, rv)
	case Binary:
		return compareBinaries(lv, rv)
	case ObjectID:
		return compareObjectIDs(lv, rv)
	case Boolean:
		return compareBooleans(lv, rv)
	case Date:
		return compareDates(lv, rv)
	case Timestamp:
		return compareTimestamps(lv, rv)
	case Regex:
		return compareRegexes(lv, rv)
	}

	return 0, fmt.Errorf("compare: unreachable")
}

func compareNumbers(lv, rv interface{}) (int, error) {
	switch l := lv.(type) {
	case float64:
		switch r := rv.(type) {
		case float64:
			return compareFloat64s(l, r), nil
		case int32:
			return compareFloat64s(l, float64(r)), nil
		case int64:
			return compareFloat64ToInt64(l, r), nil
		}
	case int32:
		switch r := rv.(type) {
		case float64:
			return compareFloat64s(float64(l), r), nil
		case int32:
			return compareInt32s(l, r), nil
		case int64:
			return compareInt64s(int64(l), r), nil
		}
	case int64:
		switch r := rv.(type) {
		case float64:
			return compareInt64ToFloat64(l, r), nil
		case int32:
			return compareInt64s(l, int64(r)), nil
		case int64:
			return compareInt64s(l, r), nil
		}
	}

	return 0, fmt.Errorf("compare: unreachable")
}

func compareStrings(lv, rv interface{}) (int, error) {
	// get strings
	l, lo := lv.(string)
	r, ro := rv.(string)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected strings")
	}

	// compare strings
	res := strings.Compare(l, r)

	return res, nil
}

func compareObjects(lv, rv interface{}) (int, error) {
	// get documents
	l, lo := lv.(bson.D)
	r, ro := rv.(bson.D)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected documents")
	}

	// handle emptiness
	if len(l) == 0 {
		if len(r) == 0 {
			return 0, nil
		} else {
			return -1, nil
		}
	} else if len(r) == 0 {
		return 1, nil
	}

	// compare document elements
	for i := 0; ; i++ {
		// handle exhaustion
		if i == len(l) {
			if i == len(r) {
				return 0, nil
			} else {
				return -1, nil
			}
		} else if i == len(r) {
			return 1, nil
		}

		// compare keys
		res := strings.Compare(l[i].Key, r[i].Key)
		if res != 0 {
			return res, nil
		}

		// compare values
		res, err := Compare(l[i].Value, r[i].Value)
		if err != nil {
			return 0, err
		} else if res != 0 {
			return res, nil
		}
	}
}

func compareArrays(lv, rv interface{}) (int, error) {
	// get array
	l, lo := lv.(bson.A)
	r, ro := rv.(bson.A)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected arrays")
	}

	// handle emptiness
	if len(l) == 0 {
		if len(r) == 0 {
			return 0, nil
		} else {
			return -1, nil
		}
	} else if len(r) == 0 {
		return 1, nil
	}

	// compare array elements
	for i := 0; ; i++ {
		// handle exhaustion
		if i == len(l) {
			if i == len(r) {
				return 0, nil
			} else {
				return -1, nil
			}
		} else if i == len(r) {
			return 1, nil
		}

		// compare elements
		res, err := Compare(l[i], r[i])
		if err != nil {
			return 0, err
		} else if res != 0 {
			return res, nil
		}
	}
}

func compareBinaries(lv, rv interface{}) (int, error) {
	// get bytes
	l, lo := lv.([]byte)
	r, ro := rv.([]byte)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected bytes")
	}

	// compare bytes
	res := bytes.Compare(l, r)

	return res, nil
}

func compareObjectIDs(lv, rv interface{}) (int, error) {
	// get object ids
	l, lo := lv.(primitive.ObjectID)
	r, ro := rv.(primitive.ObjectID)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected object ids")
	}

	// compare object ids
	res := bytes.Compare(l[:], r[:])

	return res, nil
}

func compareBooleans(lv, rv interface{}) (int, error) {
	// get booleans
	l, lo := lv.(bool)
	r, ro := rv.(bool)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected booleans")
	}

	// compare booleans
	if l == r {
		return 0, nil
	} else if l {
		return 1, nil
	} else {
		return -1, nil
	}
}

func compareDates(lv, rv interface{}) (int, error) {
	// get times
	l, lo := lv.(time.Time)
	r, ro := rv.(time.Time)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected times")
	}

	// compare times
	if l.Equal(r) {
		return 0, nil
	} else if l.After(r) {
		return 1, nil
	} else {
		return -1, nil
	}
}

func compareTimestamps(lv, rv interface{}) (int, error) {
	// get timestamps
	l, lo := lv.(primitive.Timestamp)
	r, ro := rv.(primitive.Timestamp)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected timestamps")
	}

	// compare timestamps
	ret := primitive.CompareTimestamp(l, r)

	return ret, nil
}

func compareRegexes(lv, rv interface{}) (int, error) {
	// get regexes
	l, lo := lv.(primitive.Regex)
	r, ro := rv.(primitive.Regex)
	if !lo || !ro {
		return 0, fmt.Errorf("compare: expected regexes")
	}

	// compare patterns
	ret := strings.Compare(l.Pattern, r.Pattern)
	if ret > 0 {
		return ret, nil
	}

	// compare options
	ret = strings.Compare(l.Options, r.Options)

	return ret, nil
}

// https://github.com/mongodb/mongo/blob/master/src/mongo/base/compare_numbers.h

func compareInt32s(l int32, r int32) int {
	if l == r {
		return 0
	} else if l > r {
		return 1
	}

	return -1
}

func compareInt64s(l int64, r int64) int {
	if l == r {
		return 0
	} else if l > r {
		return 1
	}

	return -1
}

func compareFloat64s(l float64, r float64) int {
	if l == r {
		return 0
	} else if l > r {
		return 1
	} else if l < r {
		return -1
	}

	// NaN values are smaller
	if math.IsNaN(l) {
		if math.IsNaN(r) {
			return 0
		} else {
			return -1
		}
	}

	return 1
}

const maxPreciseFloat64 = int64(1 << 53)
const maxMagnitude = float64(2 << 63)

func compareInt64ToFloat64(l int64, r float64) int {
	// NaN value are always smaller
	if math.IsNaN(r) {
		return 1
	}

	// compare as floats64 if not too big
	if l <= maxPreciseFloat64 && l >= -maxPreciseFloat64 {
		return compareFloat64s(float64(l), r)
	}

	// large magnitude doubles (including +/- Inf) are strictly > or < all Longs.
	if r >= maxMagnitude {
		return -1
	} else if r < -maxMagnitude {
		return 1
	}

	return compareInt64s(l, int64(r))
}

func compareFloat64ToInt64(l float64, r int64) int {
	return -compareInt64ToFloat64(r, l)
}
