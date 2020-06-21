package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdd(t *testing.T) {
	assert.Equal(t, Missing, Add("x", "y"))
	assert.Equal(t, Missing, Add(int32(2), "y"))
	assert.Equal(t, Missing, Add("x", int32(2)))

	assert.Equal(t, int32(4), Add(int32(2), int32(2)))
	assert.Equal(t, int64(4), Add(int32(2), int64(2)))
	assert.Equal(t, float64(4), Add(int32(2), float64(2)))
	assert.Equal(t, int64(4), Add(int64(2), int32(2)))
	assert.Equal(t, int64(4), Add(int64(2), int64(2)))
	assert.Equal(t, float64(4), Add(int64(2), float64(2)))
	assert.Equal(t, float64(4), Add(float64(2), int32(2)))
	assert.Equal(t, float64(4), Add(float64(2), int64(2)))
	assert.Equal(t, float64(4), Add(float64(2), float64(2)))
}

func TestMul(t *testing.T) {
	assert.Equal(t, Missing, Mul("x", "y"))
	assert.Equal(t, Missing, Mul(int32(2), "y"))
	assert.Equal(t, Missing, Mul("x", int32(2)))

	assert.Equal(t, int32(4), Mul(int32(2), int32(2)))
	assert.Equal(t, int64(4), Mul(int32(2), int64(2)))
	assert.Equal(t, float64(4), Mul(int32(2), float64(2)))
	assert.Equal(t, int64(4), Mul(int64(2), int32(2)))
	assert.Equal(t, int64(4), Mul(int64(2), int64(2)))
	assert.Equal(t, float64(4), Mul(int64(2), float64(2)))
	assert.Equal(t, float64(4), Mul(float64(2), int32(2)))
	assert.Equal(t, float64(4), Mul(float64(2), int64(2)))
	assert.Equal(t, float64(4), Mul(float64(2), float64(2)))
}

func TestMod(t *testing.T) {
	assert.Equal(t, Missing, Mod("x", "y"))
	assert.Equal(t, Missing, Mod(int32(2), "y"))
	assert.Equal(t, Missing, Mod("x", int32(2)))

	assert.Equal(t, int32(0), Mod(int32(2), int32(2)))
	assert.Equal(t, int64(0), Mod(int32(2), int64(2)))
	assert.Equal(t, float64(0), Mod(int32(2), float64(2)))
	assert.Equal(t, int64(0), Mod(int64(2), int32(2)))
	assert.Equal(t, int64(0), Mod(int64(2), int64(2)))
	assert.Equal(t, float64(0), Mod(int64(2), float64(2)))
	assert.Equal(t, float64(0), Mod(float64(2), int32(2)))
	assert.Equal(t, float64(0), Mod(float64(2), int64(2)))
	assert.Equal(t, float64(0), Mod(float64(2), float64(2)))
}
