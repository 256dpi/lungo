package lungo

import "context"

var _ Cursor = &AltCursor{}

type AltCursor struct {
}

func (c *AltCursor) All(context.Context, interface{}) error {
	panic("not implemented")
}

func (c *AltCursor) Close(context.Context) error {
	panic("not implemented")
}

func (c *AltCursor) Decode(interface{}) error {
	panic("not implemented")
}

func (c *AltCursor) Err() error {
	panic("not implemented")
}

func (c *AltCursor) ID() int64 {
	panic("not implemented")
}

func (c *AltCursor) Next(context.Context) bool {
	panic("not implemented")
}
