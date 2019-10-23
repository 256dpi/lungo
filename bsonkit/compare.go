package bsonkit

import (
	"bytes"
	"math"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func Compare(lv, rv interface{}) int {
	// get types
	lt := Inspect(lv)
	rt := Inspect(rv)

	// check type equality
	if lt > rt {
		return 1
	} else if lt < rt {
		return -1
	}

	// check value equality
	switch lt {
	case Null:
		return 0
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
	default:
		panic("bsonkit: unreachable")
	}
}

func compareNumbers(lv, rv interface{}) int {
	switch l := lv.(type) {
	case float64:
		switch r := rv.(type) {
		case float64:
			return compareFloat64s(l, r)
		case int32:
			return compareFloat64s(l, float64(r))
		case int64:
			return compareFloat64ToInt64(l, r)
		}
	case int32:
		switch r := rv.(type) {
		case float64:
			return compareFloat64s(float64(l), r)
		case int32:
			return compareInt32s(l, r)
		case int64:
			return compareInt64s(int64(l), r)
		}
	case int64:
		switch r := rv.(type) {
		case float64:
			return compareInt64ToFloat64(l, r)
		case int32:
			return compareInt64s(l, int64(r))
		case int64:
			return compareInt64s(l, r)
		}
	}

	panic("bsonkit: unreachable")
}

func compareStrings(lv, rv interface{}) int {
	// get strings
	l := lv.(string)
	r := rv.(string)

	// compare strings
	res := strings.Compare(l, r)

	return res
}

func compareObjects(lv, rv interface{}) int {
	// get documents
	l := lv.(bson.D)
	r := rv.(bson.D)

	// handle emptiness
	if len(l) == 0 {
		if len(r) == 0 {
			return 0
		} else {
			return -1
		}
	} else if len(r) == 0 {
		return 1
	}

	// compare document elements
	for i := 0; ; i++ {
		// handle exhaustion
		if i == len(l) {
			if i == len(r) {
				return 0
			} else {
				return -1
			}
		} else if i == len(r) {
			return 1
		}

		// compare keys
		res := strings.Compare(l[i].Key, r[i].Key)
		if res != 0 {
			return res
		}

		// compare values
		res = Compare(l[i].Value, r[i].Value)
		if res != 0 {
			return res
		}
	}
}

func compareArrays(lv, rv interface{}) int {
	// get array
	l := lv.(bson.A)
	r := rv.(bson.A)

	// handle emptiness
	if len(l) == 0 {
		if len(r) == 0 {
			return 0
		} else {
			return -1
		}
	} else if len(r) == 0 {
		return 1
	}

	// compare array elements
	for i := 0; ; i++ {
		// handle exhaustion
		if i == len(l) {
			if i == len(r) {
				return 0
			} else {
				return -1
			}
		} else if i == len(r) {
			return 1
		}

		// compare elements
		res := Compare(l[i], r[i])
		if res != 0 {
			return res
		}
	}
}

func compareBinaries(lv, rv interface{}) int {
	// get binaries
	l := lv.(primitive.Binary)
	r := rv.(primitive.Binary)

	// compare length
	if len(l.Data) > len(r.Data) {
		return 1
	} else if len(l.Data) < len(r.Data) {
		return -1
	}

	// compare sub type
	if l.Subtype > r.Subtype {
		return 1
	} else if l.Subtype < r.Subtype {
		return -1
	}

	// compare bytes
	res := bytes.Compare(l.Data, r.Data)

	return res
}

func compareObjectIDs(lv, rv interface{}) int {
	// get object ids
	l := lv.(primitive.ObjectID)
	r := rv.(primitive.ObjectID)

	// compare object ids
	res := bytes.Compare(l[:], r[:])

	return res
}

func compareBooleans(lv, rv interface{}) int {
	// get booleans
	l := lv.(bool)
	r := rv.(bool)

	// compare booleans
	if l == r {
		return 0
	} else if l {
		return 1
	} else {
		return -1
	}
}

func compareDates(lv, rv interface{}) int {
	// get times
	l := lv.(primitive.DateTime)
	r := rv.(primitive.DateTime)

	// compare times
	if l == r {
		return 0
	} else if l > r {
		return 1
	} else {
		return -1
	}
}

func compareTimestamps(lv, rv interface{}) int {
	// get timestamps
	l := lv.(primitive.Timestamp)
	r := rv.(primitive.Timestamp)

	// compare timestamps
	ret := primitive.CompareTimestamp(l, r)

	return ret
}

func compareRegexes(lv, rv interface{}) int {
	// get regexes
	l := lv.(primitive.Regex)
	r := rv.(primitive.Regex)

	// compare patterns
	ret := strings.Compare(l.Pattern, r.Pattern)
	if ret > 0 {
		return ret
	}

	// compare options
	ret = strings.Compare(l.Options, r.Options)

	return ret
}

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

func compareInt64ToFloat64(l int64, r float64) int {
	// see the official mongodb implementation for details:
	// https://github.com/mongodb/mongo/blob/master/src/mongo/base/compare_numbers.h#L79

	// define constants
	const maxPreciseFloat64 = int64(1 << 53)
	const boundOfLongRange = float64(2 << 63)

	// non-numbers are always smaller
	if math.IsNaN(r) {
		return 1
	}

	// compare as floats64 if not too big
	if l <= maxPreciseFloat64 && l >= -maxPreciseFloat64 {
		return compareFloat64s(float64(l), r)
	}

	// large doubles (including +/- Inf) are strictly > or < all Longs.
	if r >= boundOfLongRange {
		return -1
	} else if r < -boundOfLongRange {
		return 1
	}

	return compareInt64s(l, int64(r))
}

func compareFloat64ToInt64(l float64, r int64) int {
	return -compareInt64ToFloat64(r, l)
}
