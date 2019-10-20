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
	assertUnsupported(map[string]bool{
		"CollectionOptions.ReadConcern":    opt.ReadConcern != nil,
		"CollectionOptions.WriteConcern":   opt.WriteConcern != nil,
		"CollectionOptions.ReadPreference": opt.ReadPreference != nil,
		"CollectionOptions.Registry":       opt.Registry != nil,
	})

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
	assertUnsupported(map[string]bool{
		"CountOptions.Collation": opt.Collation != nil,
		"CountOptions.Hint":      opt.Hint != nil,
		"CountOptions.Skip":      opt.Skip != nil,
	})

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
	res, err := c.client.engine.find(c.ns, query, nil, limit)
	if err != nil {
		return 0, err
	}

	return int64(len(res.matched)), nil
}

func (c *Collection) Database() IDatabase {
	return c.db
}

func (c *Collection) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	// merge options
	opt := options.MergeDeleteOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"DeleteOptions.Collation": opt.Collation != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// delete documents
	res, err := c.client.engine.delete(c.ns, query, nil, 0)
	if err != nil {
		return nil, err
	}

	return &mongo.DeleteResult{
		DeletedCount: int64(len(res.matched)),
	}, nil
}

func (c *Collection) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	// merge options
	opt := options.MergeDeleteOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"DeleteOptions.Collation": opt.Collation != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// delete document
	res, err := c.client.engine.delete(c.ns, query, nil, 1)
	if err != nil {
		return nil, err
	}

	return &mongo.DeleteResult{
		DeletedCount: int64(len(res.matched)),
	}, nil
}

func (c *Collection) Distinct(ctx context.Context, field string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
	// merge options
	opt := options.MergeDistinctOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"DistinctOptions.Collation": opt.Collation != nil,
	})

	// check field
	if field == "" {
		panic("lungo: missing field path")
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
	res, err := c.client.engine.find(c.ns, query, nil, 0)
	if err != nil {
		return nil, err
	}

	// collect distinct values
	values := bsonkit.Collect(res.matched, field, true, true)

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
	assertUnsupported(map[string]bool{
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
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// get sort
	var sort bsonkit.Doc
	if opt.Sort != nil {
		sort, err = bsonkit.Transform(opt.Sort)
		if err != nil {
			return nil, err
		}
	}

	// get limit
	var limit int
	if opt.Limit != nil {
		limit = int(*opt.Limit)
	}

	// find documents
	res, err := c.client.engine.find(c.ns, query, sort, limit)
	if err != nil {
		return nil, err
	}

	return &staticCursor{list: res.matched}, nil
}

func (c *Collection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"FindOneOptions.AllowPartialResults": opt.AllowPartialResults != nil,
		"FindOneOptions.Collation":           opt.Collation != nil,
		"FindOneOptions.CursorType":          opt.CursorType != nil,
		"FindOneOptions.Hint":                opt.Hint != nil,
		"FindOneOptions.Max":                 opt.Max != nil,
		"FindOneOptions.Min":                 opt.Min != nil,
		"FindOneOptions.Projection":          opt.Projection != nil,
		"FindOneOptions.ReturnKey":           opt.ReturnKey != nil,
		"FindOneOptions.ShowRecordID":        opt.ShowRecordID != nil,
		"FindOneOptions.Skip":                opt.Skip != nil,
		"FindOneOptions.Snapshot":            opt.Snapshot != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return &SingleResult{err: err}
	}

	// get sort
	var sort bsonkit.Doc
	if opt.Sort != nil {
		sort, err = bsonkit.Transform(opt.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// find documents
	res, err := c.client.engine.find(c.ns, query, sort, 1)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check list
	if len(res.matched) == 0 {
		return &SingleResult{}
	}

	return &SingleResult{doc: res.matched[0]}
}

func (c *Collection) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneAndDeleteOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"FindOneAndDeleteOptions.Collation":  opt.Collation != nil,
		"FindOneAndDeleteOptions.Projection": opt.Projection != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return &SingleResult{err: err}
	}

	// get sort
	var sort bsonkit.Doc
	if opt.Sort != nil {
		sort, err = bsonkit.Transform(opt.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// delete documents
	res, err := c.client.engine.delete(c.ns, query, sort, 1)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check list
	if len(res.matched) == 0 {
		return &SingleResult{}
	}

	return &SingleResult{doc: res.matched[0]}
}

func (c *Collection) FindOneAndReplace(ctx context.Context, filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneAndReplaceOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"FindOneAndReplaceOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
		"FindOneAndReplaceOptions.Collation":                opt.Collation != nil,
		"FindOneAndReplaceOptions.Projection":               opt.Projection != nil,
		"FindOneAndReplaceOptions.ReturnDocument":           opt.ReturnDocument != nil,
		"FindOneAndReplaceOptions.Upsert":                   opt.Upsert != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// check replacement
	if replacement == nil {
		panic("lungo: missing replacement document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return &SingleResult{err: err}
	}

	// get sort
	var sort bsonkit.Doc
	if opt.Sort != nil {
		sort, err = bsonkit.Transform(opt.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// transform document
	doc, err := bsonkit.Transform(replacement)
	if err != nil {
		return &SingleResult{err: err}
	}

	// insert document
	res, err := c.client.engine.replace(c.ns, query, sort, doc)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check list
	if res.replaced == nil {
		return &SingleResult{}
	}

	return &SingleResult{doc: res.matched[0]}
}

func (c *Collection) FindOneAndUpdate(ctx context.Context, filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneAndUpdateOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"FindOneAndUpdateOptions.ArrayFilters":             opt.ArrayFilters != nil,
		"FindOneAndUpdateOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
		"FindOneAndUpdateOptions.Collation":                opt.Collation != nil,
		"FindOneAndUpdateOptions.Projection":               opt.Projection != nil,
		"FindOneAndUpdateOptions.ReturnDocument":           opt.ReturnDocument != nil,
		"FindOneAndUpdateOptions.Upsert":                   opt.Upsert != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// check update
	if update == nil {
		panic("lungo: missing update document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return &SingleResult{err: err}
	}

	// get sort
	var sort bsonkit.Doc
	if opt.Sort != nil {
		sort, err = bsonkit.Transform(opt.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// transform document
	doc, err := bsonkit.Transform(update)
	if err != nil {
		return &SingleResult{err: err}
	}

	// update documents
	res, err := c.client.engine.update(c.ns, query, sort, doc, 1)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check list
	if len(res.updated) == 0 {
		return &SingleResult{}
	}

	return &SingleResult{doc: res.matched[0]}
}

func (c *Collection) Indexes() mongo.IndexView {
	panic("not implemented")
}

func (c *Collection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	// merge options
	opt := options.MergeInsertManyOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"InsertManyOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
	})

	// check documents
	if len(documents) == 0 {
		panic("lungo: missing documents")
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
	err := c.client.engine.insert(c.ns, docs)
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
	assertUnsupported(map[string]bool{
		"InsertOneOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
	})

	// check document
	if document == nil {
		panic("lungo: missing document")
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
	assertUnsupported(map[string]bool{
		"ReplaceOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
		"ReplaceOptions.Collation":                opt.Collation != nil,
		"ReplaceOptions.Upsert":                   opt.Upsert != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// check replacement
	if replacement == nil {
		panic("lungo: missing replacement document")
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

	// insert document
	res, err := c.client.engine.replace(c.ns, query, nil, doc)
	if err != nil {
		return nil, err
	}

	// check list
	if res.replaced == nil {
		return &mongo.UpdateResult{
			MatchedCount:  0,
			ModifiedCount: 0,
		}, nil
	}

	return &mongo.UpdateResult{
		MatchedCount:  1,
		ModifiedCount: 1,
	}, nil
}

func (c *Collection) UpdateMany(ctx context.Context, filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	// merge options
	opt := options.MergeUpdateOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"UpdateOptions.ArrayFilters":             opt.ArrayFilters != nil,
		"UpdateOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
		"UpdateOptions.Collation":                opt.Collation != nil,
		"UpdateOptions.Upsert":                   opt.Upsert != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// check update
	if update == nil {
		panic("lungo: missing update document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// transform document
	doc, err := bsonkit.Transform(update)
	if err != nil {
		return nil, err
	}

	// update documents
	res, err := c.client.engine.update(c.ns, query, nil, doc, 0)
	if err != nil {
		return nil, err
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(len(res.matched)),
		ModifiedCount: int64(len(res.updated)),
	}, nil
}

func (c *Collection) UpdateOne(ctx context.Context, filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	// merge options
	opt := options.MergeUpdateOptions(opts...)

	// assert unsupported options
	assertUnsupported(map[string]bool{
		"UpdateOptions.ArrayFilters":             opt.ArrayFilters != nil,
		"UpdateOptions.BypassDocumentValidation": opt.BypassDocumentValidation != nil,
		"UpdateOptions.Collation":                opt.Collation != nil,
		"UpdateOptions.Upsert":                   opt.Upsert != nil,
	})

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// check update
	if update == nil {
		panic("lungo: missing update document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// transform document
	doc, err := bsonkit.Transform(update)
	if err != nil {
		return nil, err
	}

	// update documents
	res, err := c.client.engine.update(c.ns, query, nil, doc, 1)
	if err != nil {
		return nil, err
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(len(res.matched)),
		ModifiedCount: int64(len(res.updated)),
	}, nil
}

func (c *Collection) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("not implemented")
}
