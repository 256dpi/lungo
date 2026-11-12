package bsonkit

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCompare(t *testing.T) {
	// equality
	assert.Equal(t, 0, Compare(bson.D{}, bson.D{}))

	// less than
	assert.Equal(t, -1, Compare("foo", false))

	// greater than
	assert.Equal(t, 1, Compare(false, "foo"))

	// decimal
	dec, err := bson.ParseDecimal128("3.14")
	assert.NoError(t, err)
	assert.Equal(t, 1, Compare(5.0, dec))

	// regex pattern less / greater / equal
	assert.Equal(t, -1, Compare(
		bson.Regex{Pattern: "abc"},
		bson.Regex{Pattern: "xyz"},
	))
	assert.Equal(t, 1, Compare(
		bson.Regex{Pattern: "xyz"},
		bson.Regex{Pattern: "abc"},
	))
	assert.Equal(t, 0, Compare(
		bson.Regex{Pattern: "abc", Options: "i"},
		bson.Regex{Pattern: "abc", Options: "i"},
	))

	// regex options break equality
	assert.Equal(t, -1, Compare(
		bson.Regex{Pattern: "abc", Options: "i"},
		bson.Regex{Pattern: "abc", Options: "im"},
	))

	// a float at 2^63 is strictly greater than every int64 (max is 2^63-1).
	// The previous bound constant was off by a factor of two and let these
	// floats fall through to an int64(r) cast that saturates to MinInt64.
	bigFloat := math.Pow(2, 63)
	assert.Equal(t, -1, Compare(int64(math.MaxInt64), bigFloat))
	assert.Equal(t, 1, Compare(bigFloat, int64(math.MaxInt64)))
}

func TestCompareNonFiniteNumbersNoPanic(t *testing.T) {
	floats := []float64{math.NaN(), math.Inf(1), math.Inf(-1)}
	specials := []bson.Decimal128{d128("NaN"), d128("Infinity"), d128("-Infinity")}

	for _, f := range floats {
		for _, d := range specials {
			assert.NotPanics(t, func() { Compare(f, d) })
			assert.NotPanics(t, func() { Compare(d, f) })
		}
		// finite Decimal128 against non-finite float
		assert.NotPanics(t, func() { Compare(f, d128("1")) })
		assert.NotPanics(t, func() { Compare(d128("1"), f) })
	}

	for _, d := range specials {
		// finite float against non-finite Decimal128
		assert.NotPanics(t, func() { Compare(float64(1), d) })
		assert.NotPanics(t, func() { Compare(d, float64(1)) })

		// integer types against non-finite Decimal128
		assert.NotPanics(t, func() { Compare(int32(1), d) })
		assert.NotPanics(t, func() { Compare(d, int64(1)) })
	}
}
