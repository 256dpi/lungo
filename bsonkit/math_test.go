package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func d128(str string) primitive.Decimal128 {
	d, err := primitive.ParseDecimal128(str)
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

func TestAddLax(t *testing.T) {
	assert.Equal(t, int64(4), AddLax(2, uint16(2)))
	assert.Equal(t, d128("4"), AddLax(float32(2), d128("2")))
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

func TestMulLax(t *testing.T) {
	assert.Equal(t, int64(4), MulLax(2, uint16(2)))
	assert.Equal(t, d128("4"), MulLax(float32(2), d128("2")))
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

func TestModLax(t *testing.T) {
	assert.Equal(t, int64(0), ModLax(2, uint16(2)))
	assert.Equal(t, d128("0"), ModLax(float32(2), d128("2")))
}
