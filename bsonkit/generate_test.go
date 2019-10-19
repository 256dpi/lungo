package bsonkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerate(t *testing.T) {
	ts1 := Generate()
	ts2 := Generate()

	assert.Equal(t, ts1.T, ts1.T)
	assert.Equal(t, ts2.I, ts1.I+1)
}
