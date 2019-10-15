package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ Collection = &AltCollection{}

type AltCollection struct {
}

func (c *AltCollection) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (Cursor, error) {
	panic("not implemented")
}

func (c *AltCollection) BulkWrite(context.Context, []mongo.WriteModel, ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	panic("not implemented")
}

func (c *AltCollection) Clone(...*options.CollectionOptions) (Collection, error) {
	panic("not implemented")
}

func (c *AltCollection) CountDocuments(context.Context, interface{}, ...*options.CountOptions) (int64, error) {
	panic("not implemented")
}

func (c *AltCollection) Database() Database {
	panic("not implemented")
}

func (c *AltCollection) DeleteMany(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	panic("not implemented")
}

func (c *AltCollection) DeleteOne(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	panic("not implemented")
}

func (c *AltCollection) Distinct(context.Context, string, interface{}, ...*options.DistinctOptions) ([]interface{}, error) {
	panic("not implemented")
}

func (c *AltCollection) Drop(context.Context) error {
	panic("not implemented")
}

func (c *AltCollection) EstimatedDocumentCount(context.Context, ...*options.EstimatedDocumentCountOptions) (int64, error) {
	panic("not implemented")
}

func (c *AltCollection) Find(context.Context, interface{}, ...*options.FindOptions) (Cursor, error) {
	panic("not implemented")
}

func (c *AltCollection) FindOne(context.Context, interface{}, ...*options.FindOneOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (c *AltCollection) FindOneAndDelete(context.Context, interface{}, ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (c *AltCollection) FindOneAndReplace(context.Context, interface{}, interface{}, ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (c *AltCollection) FindOneAndUpdate(context.Context, interface{}, interface{}, ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (c *AltCollection) Indexes() mongo.IndexView {
	panic("not implemented")
}

func (c *AltCollection) InsertMany(context.Context, []interface{}, ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	panic("not implemented")
}

func (c *AltCollection) InsertOne(context.Context, interface{}, ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	panic("not implemented")
}

func (c *AltCollection) Name() string {
	panic("not implemented")
}

func (c *AltCollection) ReplaceOne(context.Context, interface{}, interface{}, ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	panic("not implemented")
}

func (c *AltCollection) UpdateMany(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	panic("not implemented")
}

func (c *AltCollection) UpdateOne(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	panic("not implemented")
}

func (c *AltCollection) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("not implemented")
}
