package dbkit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSemaphore(t *testing.T) {
	sem := NewSemaphore(2)

	ok := sem.Acquire(timeout())
	assert.True(t, ok)

	ok = sem.Acquire(timeout())
	assert.True(t, ok)

	ok = sem.Acquire(timeout())
	assert.False(t, ok)

	sem.Release()
	sem.Release()

	assert.Panics(t, func() {
		sem.Release()
	})
}

func timeout() chan struct{} {
	done := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(done)
	}()
	return done
}
