package bsonkit

import "math"

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
