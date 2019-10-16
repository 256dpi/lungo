package lungo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ ICollection = &Collection{}

type Collection struct {
	name   string
	db     *Database
	client *Client
}

func (c *Collection) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error) {
	panic("not implemented")
}

func (c *Collection) BulkWrite(context.Context, []mongo.WriteModel, ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	panic("not implemented")
}

func (c *Collection) Clone(...*options.CollectionOptions) (ICollection, error) {
	panic("not implemented")
}

func (c *Collection) CountDocuments(context.Context, interface{}, ...*options.CountOptions) (int64, error) {
	panic("not implemented")
}

func (c *Collection) Database() IDatabase {
	return c.db
}

func (c *Collection) DeleteMany(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	panic("not implemented")
}

func (c *Collection) DeleteOne(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	panic("not implemented")
}

func (c *Collection) Distinct(context.Context, string, interface{}, ...*options.DistinctOptions) ([]interface{}, error) {
	panic("not implemented")
}

func (c *Collection) Drop(context.Context) error {
	panic("not implemented")
}

func (c *Collection) EstimatedDocumentCount(context.Context, ...*options.EstimatedDocumentCountOptions) (int64, error) {
	panic("not implemented")
}

func (c *Collection) Find(ctx context.Context, query interface{}, opts ...*options.FindOptions) (ICursor, error) {
	// merge options
	opt := options.MergeFindOptions(opts...)

	// assert unsupported options
	c.client.assertUnsupported(opt.AllowPartialResults == nil, "FindOptions.AllowPartialResults")
	c.client.assertUnsupported(opt.BatchSize == nil, "FindOptions.BatchSize")
	c.client.assertUnsupported(opt.Collation == nil, "FindOptions.Collation")
	c.client.assertUnsupported(opt.Comment == nil, "FindOptions.Comment")
	c.client.assertUnsupported(opt.CursorType == nil, "FindOptions.CursorType")
	c.client.assertUnsupported(opt.Hint == nil, "FindOptions.Hint")
	c.client.assertUnsupported(opt.Limit == nil, "FindOptions.Limit")
	c.client.assertUnsupported(opt.Max == nil, "FindOptions.Max")
	c.client.assertUnsupported(opt.MaxAwaitTime == nil, "FindOptions.MaxAwaitTime")
	c.client.assertUnsupported(opt.MaxTime == nil, "FindOptions.MaxTime")
	c.client.assertUnsupported(opt.Min == nil, "FindOptions.Min")
	c.client.assertUnsupported(opt.NoCursorTimeout == nil, "FindOptions.NoCursorTimeout")
	c.client.assertUnsupported(opt.OplogReplay == nil, "FindOptions.OplogReplay")
	c.client.assertUnsupported(opt.Projection == nil, "FindOptions.Projection")
	c.client.assertUnsupported(opt.ReturnKey == nil, "FindOptions.ReturnKey")
	c.client.assertUnsupported(opt.ShowRecordID == nil, "FindOptions.ShowRecordID")
	c.client.assertUnsupported(opt.Skip == nil, "FindOptions.Skip")
	c.client.assertUnsupported(opt.Snapshot == nil, "FindOptions.Snapshot")
	c.client.assertUnsupported(opt.Sort == nil, "FindOptions.Sort")

	// reduce query
	qry, err := ReduceDocument(query)
	if err != nil {
		return nil, err
	}

	// TODO: Check supported operators.

	// get cursor
	csr, err := c.client.backend.find(c.db.name, c.name, qry)
	if err != nil {
		return nil, err
	}

	return csr, nil
}

func (c *Collection) FindOne(context.Context, interface{}, ...*options.FindOneOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (c *Collection) FindOneAndDelete(context.Context, interface{}, ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (c *Collection) FindOneAndReplace(context.Context, interface{}, interface{}, ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (c *Collection) FindOneAndUpdate(context.Context, interface{}, interface{}, ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
	panic("not implemented")
}

func (c *Collection) Indexes() mongo.IndexView {
	panic("not implemented")
}

func (c *Collection) InsertMany(context.Context, []interface{}, ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	panic("not implemented")
}

func (c *Collection) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	// merge options
	opt := options.MergeInsertOneOptions(opts...)

	// assert unsupported options
	c.client.assertUnsupported(opt.BypassDocumentValidation == nil, "InsertOneOptions.BypassDocumentValidation")

	// reduce document
	doc, err := ReduceDocument(document)
	if err != nil {
		return nil, err
	}

	// check & ensure id
	if doc["_id"] != nil {
		if _, ok := doc["_id"].(primitive.ObjectID); !ok {
			return nil, fmt.Errorf("only primitive.OjectID values are supported in _id field")
		}
	} else {
		doc["_id"] = primitive.NewObjectID()
	}

	// write document
	err = c.client.backend.insertOne(c.db.name, c.name, doc)
	if err != nil {
		return nil, err
	}

	return &mongo.InsertOneResult{
		InsertedID: doc["_id"],
	}, nil
}

func (c *Collection) Name() string {
	panic("not implemented")
}

func (c *Collection) ReplaceOne(context.Context, interface{}, interface{}, ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	panic("not implemented")
}

func (c *Collection) UpdateMany(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	panic("not implemented")
}

func (c *Collection) UpdateOne(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	panic("not implemented")
}

func (c *Collection) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("not implemented")
}
