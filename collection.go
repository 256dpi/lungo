package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
	"github.com/256dpi/lungo/mongokit"
)

var _ ICollection = &Collection{}

// Collection wraps an Engine to be mongo compatible.
type Collection struct {
	engine *Engine
	handle Handle
}

// Aggregate implements the ICollection.Aggregate method.
func (c *Collection) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error) {
	panic("lungo: not implemented")
}

// BulkWrite implements the ICollection.BulkWrite method.
func (c *Collection) BulkWrite(context.Context, []mongo.WriteModel, ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	panic("lungo: not implemented")
}

// Clone implements the ICollection.Clone method.
func (c *Collection) Clone(opts ...*options.CollectionOptions) (ICollection, error) {
	// merge options
	opt := options.MergeCollectionOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"ReadConcern":    ignored,
		"WriteConcern":   ignored,
		"ReadPreference": ignored,
	})

	return &Collection{
		engine: c.engine,
		handle: c.handle,
	}, nil
}

// CountDocuments implements the ICollection.CountDocuments method.
func (c *Collection) CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	// merge options
	opt := options.MergeCountOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"Limit":   supported,
		"MaxTime": ignored,
		"Skip":    supported,
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

	// get skip
	var skip int
	if opt.Skip != nil {
		skip = int(*opt.Skip)
	}

	// get limit
	var limit int
	if opt.Limit != nil {
		limit = int(*opt.Limit)
	}

	// find documents
	res, err := c.engine.Find(c.handle, query, nil, skip, limit)
	if err != nil {
		return 0, err
	}

	return int64(len(res.Matched)), nil
}

// Database implements the ICollection.Database method.
func (c *Collection) Database() IDatabase {
	return &Database{
		name:   c.handle[0],
		engine: c.engine,
	}
}

// DeleteMany implements the ICollection.DeleteMany method.
func (c *Collection) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	// merge options
	opt := options.MergeDeleteOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

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
	res, err := c.engine.Delete(c.handle, query, nil, 0)
	if err != nil {
		return nil, err
	}

	return &mongo.DeleteResult{
		DeletedCount: int64(len(res.Matched)),
	}, nil
}

// DeleteOne implements the ICollection.DeleteOne method.
func (c *Collection) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	// merge options
	opt := options.MergeDeleteOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

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
	res, err := c.engine.Delete(c.handle, query, nil, 1)
	if err != nil {
		return nil, err
	}

	return &mongo.DeleteResult{
		DeletedCount: int64(len(res.Matched)),
	}, nil
}

// Distinct implements the ICollection.Distinct method.
func (c *Collection) Distinct(ctx context.Context, field string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
	// merge options
	opt := options.MergeDistinctOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
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
	res, err := c.engine.Find(c.handle, query, nil, 0, 0)
	if err != nil {
		return nil, err
	}

	// collect distinct values
	values := mongokit.Distinct(res.Matched, field)

	return values, nil
}

// Drop implements the ICollection.Drop method.
func (c *Collection) Drop(context.Context) error {
	// drop namespace
	err := c.engine.Drop(c.handle)
	if err != nil {
		return err
	}

	return nil
}

// EstimatedDocumentCount implements the ICollection.EstimatedDocumentCount method.
func (c *Collection) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	// merge options
	opt := options.MergeEstimatedDocumentCountOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
	})

	// get num documents
	num := c.engine.NumDocuments(c.handle)

	return int64(num), nil
}

// Find implements the ICollection.Find method.
func (c *Collection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (ICursor, error) {
	// merge options
	opt := options.MergeFindOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"AllowPartialResults": ignored,
		"BatchSize":           ignored,
		"Comment":             ignored,
		"Limit":               supported,
		"MaxAwaitTime":        ignored,
		"MaxTime":             ignored,
		"NoCursorTimeout":     ignored,
		"Skip":                supported,
		"Snapshot":            ignored,
		"Sort":                supported,
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

	// get skip
	var skip int
	if opt.Skip != nil {
		skip = int(*opt.Skip)
	}

	// get limit
	var limit int
	if opt.Limit != nil {
		limit = int(*opt.Limit)
	}

	// find documents
	res, err := c.engine.Find(c.handle, query, sort, skip, limit)
	if err != nil {
		return nil, err
	}

	return &Cursor{list: res.Matched}, nil
}

// FindOne implements the ICollection.FindOne method.
func (c *Collection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"AllowPartialResults": ignored,
		"BatchSize":           ignored,
		"Comment":             ignored,
		"MaxAwaitTime":        ignored,
		"MaxTime":             ignored,
		"NoCursorTimeout":     ignored,
		"Skip":                supported,
		"Snapshot":            ignored,
		"Sort":                supported,
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

	// get skip
	var skip int
	if opt.Skip != nil {
		skip = int(*opt.Skip)
	}

	// find documents
	res, err := c.engine.Find(c.handle, query, sort, skip, 1)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check list
	if len(res.Matched) == 0 {
		return &SingleResult{}
	}

	return &SingleResult{doc: res.Matched[0]}
}

// FindOneAndDelete implements the ICollection.FindOneAndDelete method.
func (c *Collection) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneAndDeleteOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime": ignored,
		"Sort":    supported,
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
	res, err := c.engine.Delete(c.handle, query, sort, 1)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check list
	if len(res.Matched) == 0 {
		return &SingleResult{}
	}

	return &SingleResult{doc: res.Matched[0]}
}

// FindOneAndReplace implements the ICollection.FindOneAndReplace method.
func (c *Collection) FindOneAndReplace(ctx context.Context, filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneAndReplaceOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime":        ignored,
		"ReturnDocument": supported,
		"Sort":           supported,
		"Upsert":         supported,
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

	// get upsert
	var upsert bool
	if opt.Upsert != nil {
		upsert = *opt.Upsert
	}

	// get return after
	var returnAfter bool
	if opt.ReturnDocument != nil {
		returnAfter = *opt.ReturnDocument == options.After
	}

	// insert document
	res, err := c.engine.Replace(c.handle, query, sort, doc, upsert)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check if upserted
	if res.Upserted != nil {
		if returnAfter {
			return &SingleResult{doc: res.Upserted}
		}

		return &SingleResult{}
	}

	// check if replaced
	if len(res.Modified) > 0 {
		if returnAfter {
			return &SingleResult{doc: res.Modified[0]}
		}

		return &SingleResult{doc: res.Matched[0]}
	}

	return &SingleResult{}
}

// FindOneAndUpdate implements the ICollection.FindOneAndUpdate method.
func (c *Collection) FindOneAndUpdate(ctx context.Context, filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) ISingleResult {
	// merge options
	opt := options.MergeFindOneAndUpdateOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"MaxTime":        ignored,
		"ReturnDocument": supported,
		"Sort":           supported,
		"Upsert":         supported,
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

	// get upsert
	var upsert bool
	if opt.Upsert != nil {
		upsert = *opt.Upsert
	}

	// get return after
	var returnAfter bool
	if opt.ReturnDocument != nil {
		returnAfter = *opt.ReturnDocument == options.After
	}

	// update documents
	res, err := c.engine.Update(c.handle, query, sort, doc, 1, upsert)
	if err != nil {
		return &SingleResult{err: err}
	}

	// check if upserted
	if res.Upserted != nil {
		if returnAfter {
			return &SingleResult{doc: res.Upserted}
		}

		return &SingleResult{}
	}

	// check list
	if len(res.Modified) > 0 {
		if returnAfter {
			return &SingleResult{doc: res.Modified[0]}
		}

		return &SingleResult{doc: res.Matched[0]}
	}

	return &SingleResult{}
}

// Indexes implements the ICollection.Indexes method.
func (c *Collection) Indexes() IIndexView {
	return &IndexView{
		handle: c.handle,
		engine: c.engine,
	}
}

// InsertMany implements the ICollection.InsertMany method.
func (c *Collection) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	// merge options
	opt := options.MergeInsertManyOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"Ordered": supported,
	})

	// check documents
	if len(documents) == 0 {
		panic("lungo: missing documents")
	}

	// prepare list
	list := make(bsonkit.List, 0, len(documents))

	// transform documents
	for _, document := range documents {
		// transform document
		doc, err := bsonkit.Transform(document)
		if err != nil {
			return nil, err
		}

		// add to list
		list = append(list, doc)
	}

	// get ordered
	var ordered bool
	if opt.Ordered != nil {
		ordered = *opt.Ordered
	}

	// insert documents
	res, err := c.engine.Insert(c.handle, list, ordered)
	if err != nil {
		return nil, err
	}

	// get first error
	if len(res.Errors) > 0 {
		err = res.Errors[0]
	}

	return &mongo.InsertManyResult{
		InsertedIDs: bsonkit.Pick(res.Modified, "_id", false),
	}, err
}

// InsertOne implements the ICollection.InsertOne method.
func (c *Collection) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	// merge options
	opt := options.MergeInsertOneOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

	// check document
	if document == nil {
		panic("lungo: missing document")
	}

	// transform document
	doc, err := bsonkit.Transform(document)
	if err != nil {
		return nil, err
	}

	// insert document
	res, err := c.engine.Insert(c.handle, bsonkit.List{doc}, true)
	if err != nil {
		return nil, err
	} else if len(res.Errors) > 0 {
		return nil, res.Errors[0]
	}

	return &mongo.InsertOneResult{
		InsertedID: bsonkit.Get(res.Modified[0], "_id"),
	}, nil
}

// Name implements the ICollection.Name method.
func (c *Collection) Name() string {
	return c.handle[1]
}

// ReplaceOne implements the ICollection.ReplaceOne method.
func (c *Collection) ReplaceOne(ctx context.Context, filter, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	// merge options
	opt := options.MergeReplaceOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"Upsert": supported,
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

	// get upsert
	var upsert bool
	if opt.Upsert != nil {
		upsert = *opt.Upsert
	}

	// insert document
	res, err := c.engine.Replace(c.handle, query, nil, doc, upsert)
	if err != nil {
		return nil, err
	}

	// check if upserted
	if res.Upserted != nil {
		return &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    bsonkit.Get(res.Upserted, "_id"),
		}, nil
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(len(res.Matched)),
		ModifiedCount: int64(len(res.Modified)),
	}, nil
}

// UpdateMany implements the ICollection.UpdateMany method.
func (c *Collection) UpdateMany(ctx context.Context, filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	// merge options
	opt := options.MergeUpdateOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"Upsert": supported,
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

	// get upsert
	var upsert bool
	if opt.Upsert != nil {
		upsert = *opt.Upsert
	}

	// update documents
	res, err := c.engine.Update(c.handle, query, nil, doc, 0, upsert)
	if err != nil {
		return nil, err
	}

	// check if upserted
	if res.Upserted != nil {
		return &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    bsonkit.Get(res.Upserted, "_id"),
		}, nil
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(len(res.Matched)),
		ModifiedCount: int64(len(res.Modified)),
	}, nil
}

// UpdateOne implements the ICollection.UpdateOne method.
func (c *Collection) UpdateOne(ctx context.Context, filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	// merge options
	opt := options.MergeUpdateOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"Upsert": supported,
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

	// get upsert
	var upsert bool
	if opt.Upsert != nil {
		upsert = *opt.Upsert
	}

	// update documents
	res, err := c.engine.Update(c.handle, query, nil, doc, 1, upsert)
	if err != nil {
		return nil, err
	}

	// check if upserted
	if res.Upserted != nil {
		return &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    bsonkit.Get(res.Upserted, "_id"),
		}, nil
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(len(res.Matched)),
		ModifiedCount: int64(len(res.Modified)),
	}, nil
}

// Watch implements the ICollection.Watch method.
func (c *Collection) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("lungo: not implemented")
}
