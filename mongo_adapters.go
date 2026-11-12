package lungo

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type driverCursor struct {
	*mongo.Cursor
}

func (c *driverCursor) SetMaxTime(time.Duration) {}

func wrapCursor(c *mongo.Cursor) ICursor {
	if c == nil {
		return nil
	}
	return &driverCursor{Cursor: c}
}

type driverSingleResult struct {
	*mongo.SingleResult
}

func (sr *driverSingleResult) DecodeBytes() (bson.Raw, error) {
	return sr.Raw()
}

func wrapSingleResult(sr *mongo.SingleResult) ISingleResult {
	if sr == nil {
		return nil
	}
	return &driverSingleResult{SingleResult: sr}
}
