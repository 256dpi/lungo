package lungo

import (
	"context"

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

func (c *Collection) CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	// merge options
	opt := options.MergeCountOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"FindOptions.Collation": opt.Collation != nil,
		"FindOptions.Hint":      opt.Hint != nil,
		"FindOptions.Skip":      opt.Skip != nil,
	})
	if err != nil {
		return 0, err
	}

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return 0, err
	}

	// get limit
	var limit int
	if opt.Limit != nil {
		limit = int(*opt.Limit)
	}

	// find documents
	list, err := c.client.engine.find(c.ns, query, limit)
	if err != nil {
		return 0, err
	}

	return int64(len(list)), nil
}

func (c *Collection) Database() IDatabase {
	return c.db
}

func (c *Collection) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	// merge options
	opt := options.MergeDeleteOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"InsertOneOptions.Collation": opt.Collation != nil,
	})
	if err != nil {
		return nil, err
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// find documents
	n, err := c.client.engine.delete(c.ns, query, 0)
	if err != nil {
		return nil, err
	}

	return &mongo.DeleteResult{
		DeletedCount: int64(n),
	}, nil
}

func (c *Collection) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	// merge options
	opt := options.MergeDeleteOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"InsertOneOptions.Collation": opt.Collation != nil,
	})
	if err != nil {
		return nil, err
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// find documents
	n, err := c.client.engine.delete(c.ns, query, 1)
	if err != nil {
		return nil, err
	}

	return &mongo.DeleteResult{
		DeletedCount: int64(n),
	}, nil
}

func (c *Collection) Distinct(ctx context.Context, field string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
	// merge options
	opt := options.MergeDistinctOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"FindOptions.Collation": opt.Collation != nil,
	})
	if err != nil {
		return nil, err
	}

	// check field
	if field == "" {
		panic("lungo: empty field path")
	}

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// find documents
	list, err := c.client.engine.find(c.ns, query, 0)
	if err != nil {
		return nil, err
	}

	// collect distinct values
	values := bsonkit.Collect(list, field, true, true)

	return values, nil
}

func (c *Collection) Drop(context.Context) error {
	// drop collection
	err := c.client.engine.dropCollection(c.ns)
	if err != nil {
		return err
	}

	return nil
}

func (c *Collection) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	// get num documents
	num := c.client.engine.numDocuments(c.ns)

	return int64(num), nil
}

func (c *Collection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (ICursor, error) {
	// merge options
	opt := options.MergeFindOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"FindOptions.AllowPartialResults": opt.AllowPartialResults != nil,
		"FindOptions.Collation":           opt.Collation != nil,
		"FindOptions.CursorType":          opt.CursorType != nil,
		"FindOptions.Hint":                opt.Hint != nil,
		"FindOptions.Max":                 opt.Max != nil,
		"FindOptions.Min":                 opt.Min != nil,
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

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// get limit
	var limit int
	if opt.Limit != nil {
		limit = int(*opt.Limit)
	}

	// find documents
	list, err := c.client.engine.find(c.ns, query, limit)
	if err != nil {
		return nil, err
	}

	return &staticCursor{list: list}, nil
}

func (c *Collection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"FindOptions.AllowPartialResults": opt.AllowPartialResults != nil,
		"FindOptions.Collation":           opt.Collation != nil,
		"FindOptions.CursorType":          opt.CursorType != nil,
		"FindOptions.Hint":                opt.Hint != nil,
		"FindOptions.Max":                 opt.Max != nil,
		"FindOptions.Min":                 opt.Min != nil,
		"FindOptions.Projection":          opt.Projection != nil,
		"FindOptions.ReturnKey":           opt.ReturnKey != nil,
		"FindOptions.ShowRecordID":        opt.ShowRecordID != nil,
		"FindOptions.Skip":                opt.Skip != nil,
		"FindOptions.Snapshot":            opt.Snapshot != nil,
		"FindOptions.Sort":                opt.Sort != nil,
	})
	if err != nil {
		return &SingleResult{err: err}
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return &SingleResult{err: err}
	}

	// find documents
	list, err := c.client.engine.find(c.ns, query, 1)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check length
	if len(list) == 0 {
		return &SingleResult{err: mongo.ErrNoDocuments}
	}

	return &SingleResult{doc: list[0]}
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

	// TODO: Handle unordered.

	// prepare lists
	docs := make(bsonkit.List, 0, len(documents))
	ids := make([]interface{}, 0, len(documents))

	// process documents
	for _, document := range documents {
		// transform document
		doc, err := bsonkit.Transform(document)
		if err != nil {
			return nil, err
		}

		// ensure object id
		id := bsonkit.Get(doc, "_id")
		if id == bsonkit.Missing {
			id = primitive.NewObjectID()
			err = bsonkit.Set(doc, "_id", id, true)
			if err != nil {
				return nil, err
			}
		}

		// add to lists
		docs = append(docs, doc)
		ids = append(ids, id)
	}

	// insert documents
	err = c.client.engine.insert(c.ns, docs)
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
	id := bsonkit.Get(doc, "_id")
	if id == bsonkit.Missing {
		id = primitive.NewObjectID()
		err = bsonkit.Set(doc, "_id", id, true)
		if err != nil {
			return nil, err
		}
	}

	// insert document
	err = c.client.engine.insert(c.ns, bsonkit.List{doc})
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
	// merge options
	opt := options.MergeReplaceOptions(opts...)

	// assert unsupported options
	err := assertUnsupported(map[string]bool{
		"InsertOneOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
		"InsertOneOptions.Collation":                opt.Collation != nil,
		"InsertOneOptions.Upsert":                   opt.Upsert != nil,
	})
	if err != nil {
		return nil, err
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// transform document
	doc, err := bsonkit.Transform(replacement)
	if err != nil {
		return nil, err
	}

	// ensure object id
	id := bsonkit.Get(doc, "_id")
	if id == bsonkit.Missing {
		id = primitive.NewObjectID()
		err = bsonkit.Set(doc, "_id", id, true)
		if err != nil {
			return nil, err
		}
	}

	// insert document
	res, err := c.client.engine.replace(c.ns, query, doc)
	if err != nil {
		return nil, err
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(res.matched),
		ModifiedCount: int64(res.modified),
	}, nil
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
