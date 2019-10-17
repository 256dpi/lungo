package lungo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

var _ ICollection = &Collection{}

type Collection struct {
	ns     string
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

func (c *Collection) Clone(opts ...*options.CollectionOptions) (ICollection, error) {
	// merge options
	opt := options.MergeCollectionOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"CollectionOptions.ReadConcern":    opt.ReadConcern != nil,
		"CollectionOptions.WriteConcern":   opt.WriteConcern != nil,
		"CollectionOptions.ReadPreference": opt.ReadPreference != nil,
		"CollectionOptions.Registry":       opt.Registry != nil,
	})
	if err != nil {
		return nil, err
	}

	return &Collection{
		ns:     c.ns,
		name:   c.name,
		db:     c.db,
		client: c.client,
	}, nil
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

func (c *Collection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (ICursor, error) {
	// merge options
	opt := options.MergeFindOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"FindOptions.AllowPartialResults": opt.AllowPartialResults != nil,
		"FindOptions.BatchSize":           opt.BatchSize != nil,
		"FindOptions.Collation":           opt.Collation != nil,
		"FindOptions.Comment":             opt.Comment != nil,
		"FindOptions.CursorType":          opt.CursorType != nil,
		"FindOptions.Hint":                opt.Hint != nil,
		"FindOptions.Limit":               opt.Limit != nil,
		"FindOptions.Max":                 opt.Max != nil,
		"FindOptions.MaxAwaitTime":        opt.MaxAwaitTime != nil,
		"FindOptions.MaxTime":             opt.MaxTime != nil,
		"FindOptions.Min":                 opt.Min != nil,
		"FindOptions.NoCursorTimeout":     opt.NoCursorTimeout != nil,
		"FindOptions.OplogReplay":         opt.OplogReplay != nil,
		"FindOptions.Projection":          opt.Projection != nil,
		"FindOptions.ReturnKey":           opt.ReturnKey != nil,
		"FindOptions.ShowRecordID":        opt.ShowRecordID != nil,
		"FindOptions.Skip":                opt.Skip != nil,
		"FindOptions.Snapshot":            opt.Snapshot != nil,
		"FindOptions.Sort":                opt.Sort != nil,
	})
	if err != nil {
		return nil, err
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// get documents
	list, err := c.client.backend.find(c.ns, query)
	if err != nil {
		return nil, err
	}

	return &staticCursor{list: list}, nil
}

func (c *Collection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) ISingleResult {
	panic("not implemented")
}

func (c *Collection) FindOneAndDelete(context.Context, interface{}, ...*options.FindOneAndDeleteOptions) ISingleResult {
	panic("not implemented")
}

func (c *Collection) FindOneAndReplace(context.Context, interface{}, interface{}, ...*options.FindOneAndReplaceOptions) ISingleResult {
	panic("not implemented")
}

func (c *Collection) FindOneAndUpdate(context.Context, interface{}, interface{}, ...*options.FindOneAndUpdateOptions) ISingleResult {
	panic("not implemented")
}

func (c *Collection) Indexes() mongo.IndexView {
	panic("not implemented")
}

func (c *Collection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	// merge options
	opt := options.MergeInsertManyOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"InsertOneOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
	})
	if err != nil {
		return nil, err
	}

	// TODO: Allow unordered.

	// prepare lists
	docs := make([]bson.D, 0, len(documents))
	ids := make([]interface{}, 0, len(documents))

	// process documents
	for _, document := range documents {
		// transform document
		doc, err := bsonkit.Transform(document)
		if err != nil {
			return nil, err
		}

		// ensure object id
		doc, id, err := ensureObjectID(doc)
		if err != nil {
			return nil, err
		}

		// add to lists
		docs = append(docs, doc)
		ids = append(ids, id)
	}

	// write documents
	err = c.client.backend.insert(c.ns, docs)
	if err != nil {
		return nil, err
	}

	return &mongo.InsertManyResult{
		InsertedIDs: ids,
	}, nil
}

func (c *Collection) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	// merge options
	opt := options.MergeInsertOneOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"InsertOneOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
	})
	if err != nil {
		return nil, err
	}

	// transform document
	doc, err := bsonkit.Transform(document)
	if err != nil {
		return nil, err
	}

	// ensure object id
	doc, id, err := ensureObjectID(doc)
	if err != nil {
		return nil, err
	}

	// write document
	err = c.client.backend.insert(c.ns, []bson.D{doc})
	if err != nil {
		return nil, err
	}

	return &mongo.InsertOneResult{
		InsertedID: id,
	}, nil
}

func (c *Collection) Name() string {
	return c.name
}

func (c *Collection) ReplaceOne(ctx context.Context, filter, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
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

func ensureObjectID(doc bson.D) (bson.D, primitive.ObjectID, error) {
	// check id
	var id primitive.ObjectID
	if v := bsonkit.Get(doc, "_id"); v != bsonkit.Missing {
		// check existing value
		oid, ok := v.(primitive.ObjectID)
		if !ok {
			return nil, oid, fmt.Errorf("only primitive.OjectID values are supported in _id field")
		} else if oid.IsZero() {
			return nil, oid, fmt.Errorf("found zero primitive.OjectID value in _id field")
		}

		// set id
		id = oid
	}

	// prepend id if zero
	if id.IsZero() {
		id = primitive.NewObjectID()
		doc = bsonkit.Set(doc, "_id", id, true)
	}

	return doc, id, nil
}
