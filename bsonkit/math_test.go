package bsonkit

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func d128(str string) bson.Decimal128 {
	d, err := bson.ParseDecimal128(str)
	if err != nil {
		panic(err)
	}
	return d
}

func TestAdd(t *testing.T) {
	assert.Equal(t, Missing, Add("x", "y"))
	assert.Equal(t, Missing, Add(int32(2), "y"))
	assert.Equal(t, Missing, Add("x", int32(2)))

	assert.Equal(t, int32(4), Add(int32(2), int32(2)))
	assert.Equal(t, int64(4), Add(int32(2), int64(2)))
	assert.Equal(t, float64(4), Add(int32(2), float64(2)))
	assert.Equal(t, d128("4"), Add(int32(2), d128("2")))

	assert.Equal(t, int64(4), Add(int64(2), int32(2)))
	assert.Equal(t, int64(4), Add(int64(2), int64(2)))
	assert.Equal(t, float64(4), Add(int64(2), float64(2)))
	assert.Equal(t, d128("4"), Add(int64(2), d128("2")))

	assert.Equal(t, float64(4), Add(float64(2), int32(2)))
	assert.Equal(t, float64(4), Add(float64(2), int64(2)))
	assert.Equal(t, float64(4), Add(float64(2), float64(2)))
	assert.Equal(t, d128("4"), Add(float64(2), d128("2")))

	assert.Equal(t, d128("4"), Add(d128("2"), int32(2)))
	assert.Equal(t, d128("4"), Add(d128("2"), int64(2)))
	assert.Equal(t, d128("4"), Add(d128("2"), float64(2)))
	assert.Equal(t, d128("4"), Add(d128("2"), d128("2")))
}

func TestMul(t *testing.T) {
	assert.Equal(t, Missing, Mul("x", "y"))
	assert.Equal(t, Missing, Mul(int32(2), "y"))
	assert.Equal(t, Missing, Mul("x", int32(2)))

	assert.Equal(t, int32(4), Mul(int32(2), int32(2)))
	assert.Equal(t, int64(4), Mul(int32(2), int64(2)))
	assert.Equal(t, float64(4), Mul(int32(2), float64(2)))
	assert.Equal(t, d128("4"), Mul(int32(2), d128("2")))

	assert.Equal(t, int64(4), Mul(int64(2), int32(2)))
	assert.Equal(t, int64(4), Mul(int64(2), int64(2)))
	assert.Equal(t, float64(4), Mul(int64(2), float64(2)))
	assert.Equal(t, d128("4"), Mul(int64(2), d128("2")))

	assert.Equal(t, float64(4), Mul(float64(2), int32(2)))
	assert.Equal(t, float64(4), Mul(float64(2), int64(2)))
	assert.Equal(t, float64(4), Mul(float64(2), float64(2)))
	assert.Equal(t, d128("4"), Mul(float64(2), d128("2")))

	assert.Equal(t, d128("4"), Mul(d128("2"), int32(2)))
	assert.Equal(t, d128("4"), Mul(d128("2"), int64(2)))
	assert.Equal(t, d128("4"), Mul(d128("2"), float64(2)))
	assert.Equal(t, d128("4"), Mul(d128("2"), d128("2")))
}

func TestMod(t *testing.T) {
	assert.Equal(t, Missing, Mod("x", "y"))
	assert.Equal(t, Missing, Mod(int32(2), "y"))
	assert.Equal(t, Missing, Mod("x", int32(2)))

	assert.Equal(t, int32(0), Mod(int32(2), int32(2)))
	assert.Equal(t, int64(0), Mod(int32(2), int64(2)))
	assert.Equal(t, float64(0), Mod(int32(2), float64(2)))
	assert.Equal(t, d128("0"), Mod(int32(2), d128("2")))

	assert.Equal(t, int64(0), Mod(int64(2), int32(2)))
	assert.Equal(t, int64(0), Mod(int64(2), int64(2)))
	assert.Equal(t, float64(0), Mod(int64(2), float64(2)))
	assert.Equal(t, d128("0"), Mod(int64(2), d128("2")))

	assert.Equal(t, float64(0), Mod(float64(2), int32(2)))
	assert.Equal(t, float64(0), Mod(float64(2), int64(2)))
	assert.Equal(t, float64(0), Mod(float64(2), float64(2)))
	assert.Equal(t, d128("0"), Mod(float64(2), d128("2")))

	assert.Equal(t, d128("0"), Mod(d128("2"), int32(2)))
	assert.Equal(t, d128("0"), Mod(d128("2"), int64(2)))
	assert.Equal(t, d128("0"), Mod(d128("2"), float64(2)))
	assert.Equal(t, d128("0"), Mod(d128("2"), d128("2")))
}

func TestArithmeticNonFiniteDecimal128(t *testing.T) {
	for _, special := range []primitive.Decimal128{d128("NaN"), d128("Infinity"), d128("-Infinity")} {
		assert.NotPanics(t, func() { Add(int32(1), special) })
		assert.NotPanics(t, func() { Add(special, d128("1")) })
		assert.NotPanics(t, func() { Mul(int32(2), special) })
		assert.NotPanics(t, func() { Mul(special, d128("2")) })
		assert.NotPanics(t, func() { Mod(int32(5), special) })
		assert.NotPanics(t, func() { Mod(special, d128("2")) })
	}
}

func TestArithmeticNonFiniteFloatWithDecimal128(t *testing.T) {
	for _, special := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		assert.NotPanics(t, func() { Add(special, d128("1")) })
		assert.NotPanics(t, func() { Add(d128("1"), special) })
		assert.NotPanics(t, func() { Mul(special, d128("2")) })
		assert.NotPanics(t, func() { Mul(d128("2"), special) })
		assert.NotPanics(t, func() { Mod(special, d128("2")) })
		assert.NotPanics(t, func() { Mod(d128("5"), special) })
	}
}

func TestModZeroDivisorNoPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.Equal(t, Missing, Mod(int32(5), int32(0)))
		assert.Equal(t, Missing, Mod(int32(5), int64(0)))
		assert.Equal(t, Missing, Mod(int64(5), int32(0)))
		assert.Equal(t, Missing, Mod(int64(5), int64(0)))
		assert.Equal(t, Missing, Mod(float64(5), int32(0)))
		assert.Equal(t, Missing, Mod(float64(5), int64(0)))
		assert.Equal(t, Missing, Mod(d128("5"), int32(0)))
		assert.Equal(t, Missing, Mod(d128("5"), int64(0)))
		assert.Equal(t, Missing, Mod(int32(5), d128("0")))
		assert.Equal(t, Missing, Mod(int64(5), d128("0")))
		assert.Equal(t, Missing, Mod(float64(5), d128("0")))
		assert.Equal(t, Missing, Mod(d128("5"), d128("0")))
	})
}
