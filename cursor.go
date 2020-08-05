package lungo

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/256dpi/lungo/bsonkit"
)

var _ ICursor = &Cursor{}

// Cursor wraps a list to be mongo compatible.
type Cursor struct {
	list   bsonkit.List
	pos    int
	closed bool
	mutex  sync.Mutex
}

// All implements the ICursor.All method.
func (c *Cursor) All(_ context.Context, out interface{}) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if c.closed {
		return fmt.Errorf("cursor closed")
	}

	// decode items
	err := bsonkit.DecodeList(c.list, out)
	if err != nil {
		return err
	}

	// close cursor
	c.closed = true

	return nil
}

// Close implements the ICursor.Close method.
func (c *Cursor) Close(context.Context) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// close cursor
	c.closed = true

	return nil
}

// Decode implements the ICursor.Decode method.
func (c *Cursor) Decode(out interface{}) error {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if exhausted
	if c.pos == 0 || c.pos > len(c.list) {
		return io.EOF
	}

	// decode item
	err := bsonkit.Decode(c.list[c.pos-1], out)
	if err != nil {
		return err
	}

	return nil
}

// Err implements the ICursor.Err method.
func (c *Cursor) Err() error {
	return nil
}

// ID implements the ICursor.ID method.
func (c *Cursor) ID() int64 {
	return 0
}

// Next implements the ICursor.Next method.
func (c *Cursor) Next(context.Context) bool {
	// acquire mutex
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// check if closed
	if c.closed {
		return false
	}

	// increment position
	if c.pos < len(c.list) {
		c.pos++
		return true
	}

	return false
}

// RemainingBatchLength implements the ICursor.RemainingBatchLength method.
func (c *Cursor) RemainingBatchLength() int {
	return len(c.list) - c.pos
}

// TryNext implements the ICursor.TryNext method.
func (c *Cursor) TryNext(ctx context.Context) bool {
	return c.Next(ctx)
}
