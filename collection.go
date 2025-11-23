package lungo

import (
	"context"
	"reflect"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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
func (c *Collection) Aggregate(
	ctx context.Context,
	pipeline any,
	opts ...options.Lister[options.AggregateOptions],
) (ICursor, error) {
	panic("lungo: not implemented")
}

// BulkWrite implements the ICollection.BulkWrite method.
func (c *Collection) BulkWrite(ctx context.Context, models []mongo.WriteModel,
	opts ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error) {
	// merge options
	args, err := NewOptions[options.BulkWriteOptions](opts...)

	if err != nil {
		panic(err)
	}

	// assert supported options
	assertOptions(args, map[string]string{
		"Ordered": supported,
	})

	// get ordered
	var ordered bool
	if args.Ordered != nil {
		ordered = *args.Ordered
	}

	// prepare operations
	ops := make([]Operation, 0, len(models))

	// transform models
	for _, item := range models {
		// prepare variables
		var opcode Opcode
		var document interface{}
		var filter interface{}
		var upsert *bool
		var limit int
		var arrayFilters []interface{}

		// set variables
		switch model := item.(type) {
		case *mongo.InsertOneModel:
			opcode = Insert
			document = model.Document
			limit = 1
		case *mongo.ReplaceOneModel:
			opcode = Replace
			filter = model.Filter
			document = model.Replacement
			upsert = model.Upsert
			limit = 1
		case *mongo.UpdateOneModel:
			opcode = Update
			filter = model.Filter
			document = model.Update
			upsert = model.Upsert
			limit = 1
			if model.ArrayFilters != nil {
				arrayFilters = model.ArrayFilters
			}
		case *mongo.UpdateManyModel:
			opcode = Update
			filter = model.Filter
			document = model.Update
			upsert = model.Upsert
			limit = 0
			if model.ArrayFilters != nil {
				arrayFilters = model.ArrayFilters
			}
		case *mongo.DeleteOneModel:
			opcode = Delete
			filter = model.Filter
			limit = 1
		case *mongo.DeleteManyModel:
			opcode = Delete
			filter = model.Filter
			limit = 0
		}

		// prepare operation
		op := Operation{
			Opcode: opcode,
			Limit:  limit,
		}

		// transform document
		if document != nil {
			doc, err := bsonkit.Transform(document)
			if err != nil {
				return nil, err
			}
			op.Document = doc
		}

		// transform filter
		if filter != nil {
			flt, err := bsonkit.Transform(filter)
			if err != nil {
				return nil, err
			}
			op.Filter = flt
		}

		// check upsert
		if upsert != nil {
			op.Upsert = *upsert
		}

		// transform array filters
		if arrayFilters != nil {
			arrFlt, err := bsonkit.TransformList(arrayFilters)
			if err != nil {
				return nil, err
			}
			op.ArrayFilters = arrFlt
		}

		// add operation
		ops = append(ops, op)
	}

	// run bulk
	results, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) ([]Result, error) {
		return txn.Bulk(c.handle, ops, ordered)
	})
	if err != nil {
		return nil, err
	}

	// prepare result
	result := &mongo.BulkWriteResult{
		InsertedCount: 0,
		MatchedCount:  0,
		ModifiedCount: 0,
		DeletedCount:  0,
		UpsertedCount: 0,
		UpsertedIDs:   map[int64]interface{}{},
	}

	// prepare errors
	errors := make(mongo.WriteErrors, 0, len(results))

	// apply bulk results
	for i, res := range results {
		// check error
		if res.Error != nil {
			errors = append(errors, mongo.WriteError{
				Index:   i,
				Code:    0,
				Message: res.Error.Error(),
			})
			continue
		}

		// collect results
		switch ops[i].Opcode {
		case Insert:
			result.InsertedCount += int64(len(res.Modified))
		case Replace:
			result.MatchedCount += int64(len(res.Matched))
			result.ModifiedCount += int64(len(res.Modified))
			if res.Upserted != nil {
				result.UpsertedCount++
				result.UpsertedIDs[int64(i)] = bsonkit.Get(res.Upserted, "_id")
			}
		case Update:
			result.MatchedCount += int64(len(res.Matched))
			result.ModifiedCount += int64(len(res.Modified))
			if res.Upserted != nil {
				result.UpsertedCount++
				result.UpsertedIDs[int64(i)] = bsonkit.Get(res.Upserted, "_id")
			}
		case Delete:
			result.DeletedCount += int64(len(res.Matched))
		}
	}

	// prepare error
	err = nil
	if len(errors) > 0 {
		err = errors
	}

	return result, err
}

// Clone implements the ICollection.Clone method.
func (c *Collection) Clone(opts ...options.Lister[options.CollectionOptions]) ICollection {
	// merge options
	args, err := NewOptions[options.CollectionOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"ReadConcern":    ignored,
		"WriteConcern":   ignored,
		"ReadPreference": ignored,
	})

	return &Collection{
		engine: c.engine,
		handle: c.handle,
	}
}

// CountDocuments implements the ICollection.CountDocuments method.
func (c *Collection) CountDocuments(ctx context.Context, filter any,
	opts ...options.Lister[options.CountOptions]) (int64, error) {
	// merge options
	args, err := NewOptions[options.CountOptions](opts...)

	if err != nil {
		panic(err)
	}

	// assert supported options
	assertOptions(args, map[string]string{
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
	if args.Skip != nil {
		skip = int(*args.Skip)
	}

	// get limit
	var limit int
	if args.Limit != nil {
		limit = int(*args.Limit)
	}

	// find documents
	res, err := useTransaction(ctx, c.engine, false, func(txn *Transaction) (*Result, error) {
		return txn.Find(c.handle, query, nil, skip, limit)
	})
	if err != nil {
		return 0, err
	}

	// get list
	list := res.Matched

	return int64(len(list)), nil
}

// Database implements the ICollection.Database method.
func (c *Collection) Database() IDatabase {
	return &Database{
		name:   c.handle[0],
		engine: c.engine,
	}
}

// DeleteMany implements the ICollection.DeleteMany method.
func (c *Collection) DeleteMany(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.DeleteManyOptions],
) (*mongo.DeleteResult, error) {
	// merge options
	args, err := NewOptions[options.DeleteManyOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{})

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
	res, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Delete(c.handle, query, nil, 0, 0)
	})
	if err != nil {
		return nil, err
	}

	// get list
	list := res.Matched

	return &mongo.DeleteResult{
		DeletedCount: int64(len(list)),
	}, nil
}

// DeleteOne implements the ICollection.DeleteOne method.
func (c *Collection) DeleteOne(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.DeleteOneOptions],
) (*mongo.DeleteResult, error) {
	// merge options
	args, err := NewOptions[options.DeleteOneOptions](opts...)

	if err != nil {
		panic(err)
	}

	// assert supported options
	assertOptions(args, map[string]string{})

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
	res, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Delete(c.handle, query, nil, 0, 1)
	})
	if err != nil {
		return nil, err
	}

	// get list
	list := res.Matched

	return &mongo.DeleteResult{
		DeletedCount: int64(len(list)),
	}, nil
}

// Distinct implements the ICollection.Distinct method.
func (c *Collection) Distinct(
	ctx context.Context,
	fieldName string,
	filter any,
	opts ...options.Lister[options.DistinctOptions],
) IDistinctResult {
	// merge options
	args, err := NewOptions[options.DistinctOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"MaxTime": ignored,
	})

	// check field
	if fieldName == "" {
		panic("lungo: missing field path")
	}

	// check filer
	if filter == nil {
		panic("lungo: missing filter document")
	}

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		//return nil, err
		panic(err)
	}

	// find documents
	res, err := useTransaction(ctx, c.engine, false, func(txn *Transaction) (*Result, error) {
		return txn.Find(c.handle, query, nil, 0, 0)
	})
	if err != nil {
		panic(err)
		//return nil, err
	}

	// get list
	list := res.Matched

	// collect distinct values
	rawValues := mongokit.Distinct(list, fieldName)
	return DistinctResult{RawArray: rawValues}
}

var _ IDistinctResult = &DistinctResult{}

type DistinctResult struct {
	bson.RawArray
}

func (d DistinctResult) Decode(v any) error {
	// if there is no underlying array, signal no documents
	if d.RawArray == nil {
		return ErrNoDocuments
	}

	// delegate decoding to the BSON RawValue helper
	return bson.RawValue{
		Type:  bson.TypeArray,
		Value: d.RawArray,
	}.Unmarshal(v)
}

func (d DistinctResult) Err() error {
	// no error state is tracked; only signal no documents if there is no array
	if d.RawArray == nil {
		return ErrNoDocuments
	}

	return nil
}

func (d DistinctResult) Raw() (bson.RawArray, error) {
	if d.RawArray == nil {
		return nil, ErrNoDocuments
	}

	return d.RawArray, nil
}

// Drop implements the ICollection.Drop method.
func (c *Collection) Drop(ctx context.Context, opts ...options.Lister[options.DropCollectionOptions]) error {
	// begin transaction
	txn, err := c.engine.Begin(ctx, true)
	if err != nil {
		return err
	}

	// ensure abortion
	defer c.engine.Abort(txn)

	// drop namespace
	err = txn.Drop(c.handle)
	if err != nil {
		return err
	}

	// commit transaction
	err = c.engine.Commit(txn)
	if err != nil {
		return err
	}

	return nil
}

// EstimatedDocumentCount implements the ICollection.EstimatedDocumentCount method.
func (c *Collection) EstimatedDocumentCount(
	ctx context.Context,
	opts ...options.Lister[options.EstimatedDocumentCountOptions],
) (int64, error) {
	// merge options
	args, err := NewOptions[options.EstimatedDocumentCountOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"MaxTime": ignored,
	})

	// count documents
	res, err := useTransaction(ctx, c.engine, false, func(txn *Transaction) (int, error) {
		return txn.CountDocuments(c.handle)
	})
	if err != nil {
		return 0, err
	}

	return int64(res), nil
}

// Find implements the ICollection.Find method.
func (c *Collection) Find(ctx context.Context, filter any,
	opts ...options.Lister[options.FindOptions]) (ICursor, error) {
	// merge options
	args, err := NewOptions[options.FindOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"AllowPartialResults": ignored,
		"BatchSize":           ignored,
		"Comment":             ignored,
		"Limit":               supported,
		"MaxAwaitTime":        ignored,
		"MaxTime":             ignored,
		"NoCursorTimeout":     ignored,
		"Projection":          supported,
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
	if args.Sort != nil {
		sort, err = bsonkit.Transform(args.Sort)
		if err != nil {
			return nil, err
		}
	}

	// get projection
	var projection bsonkit.Doc
	if args.Projection != nil {
		projection, err = bsonkit.Transform(args.Projection)
		if err != nil {
			return nil, err
		}
	}

	// get skip
	var skip int
	if args.Skip != nil {
		skip = int(*args.Skip)
	}

	// get limit
	var limit int
	if args.Limit != nil {
		limit = int(*args.Limit)
	}

	// find documents
	res, err := useTransaction(ctx, c.engine, false, func(txn *Transaction) (*Result, error) {
		return txn.Find(c.handle, query, sort, skip, limit)
	})
	if err != nil {
		return nil, err
	}

	// get list
	list := res.Matched

	// apply projection
	if projection != nil {
		list, err = mongokit.ProjectList(list, projection)
		if err != nil {
			return nil, err
		}
	}

	return &Cursor{list: list}, nil
}

// FindOne implements the ICollection.FindOne method.
func (c *Collection) FindOne(ctx context.Context, filter any,
	opts ...options.Lister[options.FindOneOptions]) ISingleResult {
	// merge options
	args, err := NewOptions[options.FindOneOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"AllowPartialResults": ignored,
		"BatchSize":           ignored,
		"Comment":             ignored,
		"MaxAwaitTime":        ignored,
		"MaxTime":             ignored,
		"NoCursorTimeout":     ignored,
		"Projection":          supported,
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
	if args.Sort != nil {
		sort, err = bsonkit.Transform(args.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// get skip
	var skip int
	if args.Skip != nil {
		skip = int(*args.Skip)
	}

	// get projection
	var projection bsonkit.Doc
	if args.Projection != nil {
		projection, err = bsonkit.Transform(args.Projection)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// find documents
	res, err := useTransaction(ctx, c.engine, false, func(txn *Transaction) (*Result, error) {
		return txn.Find(c.handle, query, sort, skip, 1)
	})
	if err != nil {
		return &SingleResult{err: err}
	}

	// get list
	list := res.Matched

	// check list
	if len(list) == 0 {
		return &SingleResult{}
	}

	// apply projection
	if projection != nil {
		list, err = mongokit.ProjectList(list, projection)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	return &SingleResult{doc: list[0]}
}

// FindOneAndDelete implements the ICollection.FindOneAndDelete method.
func (c *Collection) FindOneAndDelete(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.FindOneAndDeleteOptions]) ISingleResult {
	// merge options
	args, err := NewOptions[options.FindOneAndDeleteOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"MaxTime":    ignored,
		"Projection": supported,
		"Sort":       supported,
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

	// get projection
	var projection bsonkit.Doc
	if args.Projection != nil {
		projection, err = bsonkit.Transform(args.Projection)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// get sort
	var sort bsonkit.Doc
	if args.Sort != nil {
		sort, err = bsonkit.Transform(args.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// delete documents
	res, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Delete(c.handle, query, sort, 0, 1)
	})
	if err != nil {
		return &SingleResult{err: err}
	}

	// get list
	list := res.Matched

	// check list
	if len(list) == 0 {
		return &SingleResult{}
	}

	// apply projection
	if projection != nil {
		list, err = mongokit.ProjectList(list, projection)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	return &SingleResult{doc: list[0]}
}

// FindOneAndReplace implements the ICollection.FindOneAndReplace method.
func (c *Collection) FindOneAndReplace(
	ctx context.Context,
	filter any,
	replacement any,
	opts ...options.Lister[options.FindOneAndReplaceOptions],
) ISingleResult {
	// merge options
	args, err := NewOptions[options.FindOneAndReplaceOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"MaxTime":        ignored,
		"Projection":     supported,
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

	// get projection
	var projection bsonkit.Doc
	if args.Projection != nil {
		projection, err = bsonkit.Transform(args.Projection)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// get sort
	var sort bsonkit.Doc
	if args.Sort != nil {
		sort, err = bsonkit.Transform(args.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// transform document
	repl, err := bsonkit.Transform(replacement)
	if err != nil {
		return &SingleResult{err: err}
	}

	// get upsert
	var upsert bool
	if args.Upsert != nil {
		upsert = *args.Upsert
	}

	// get return after
	var returnAfter bool
	if args.ReturnDocument != nil {
		returnAfter = *args.ReturnDocument == options.After
	}

	// insert document
	result, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Replace(c.handle, query, sort, repl, upsert)
	})
	if err != nil {
		return &SingleResult{err: err}
	}

	// get doc
	var doc bsonkit.Doc
	if result.Upserted != nil {
		if returnAfter {
			doc = result.Upserted
		}
	} else if len(result.Modified) > 0 {
		doc = result.Matched[0]
		if returnAfter {
			doc = result.Modified[0]
		}
	}

	// apply projection
	if doc != nil && projection != nil {
		doc, err = mongokit.Project(doc, projection)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	return &SingleResult{doc: doc}
}

// FindOneAndUpdate implements the ICollection.FindOneAndUpdate method.
func (c *Collection) FindOneAndUpdate(
	ctx context.Context,
	filter any,
	update any,
	opts ...options.Lister[options.FindOneAndUpdateOptions]) ISingleResult {
	// merge options
	args, err := NewOptions[options.FindOneAndUpdateOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"MaxTime":        ignored,
		"Projection":     supported,
		"ReturnDocument": supported,
		"Sort":           supported,
		"Upsert":         supported,
		"ArrayFilters":   supported,
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

	// get projection
	var projection bsonkit.Doc
	if args.Projection != nil {
		projection, err = bsonkit.Transform(args.Projection)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// get sort
	var sort bsonkit.Doc
	if args.Sort != nil {
		sort, err = bsonkit.Transform(args.Sort)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// transform document
	upd, err := bsonkit.Transform(update)
	if err != nil {
		return &SingleResult{err: err}
	}

	// get upsert
	var upsert bool
	if args.Upsert != nil {
		upsert = *args.Upsert
	}

	// get return after
	var returnAfter bool
	if args.ReturnDocument != nil {
		returnAfter = *args.ReturnDocument == options.After
	}

	// get array filters
	var arrayFilters bsonkit.List
	if args.ArrayFilters != nil {
		arrayFilters, err = bsonkit.TransformList(args.ArrayFilters)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	// update documents
	result, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Update(c.handle, query, sort, upd, 0, 1, upsert, arrayFilters)
	})
	if err != nil {
		return &SingleResult{err: err}
	}

	// get doc
	var doc bsonkit.Doc
	if result.Upserted != nil {
		if returnAfter {
			doc = result.Upserted
		}
	} else if len(result.Modified) > 0 {
		doc = result.Matched[0]
		if returnAfter {
			doc = result.Modified[0]
		}
	}

	// apply projection
	if doc != nil && projection != nil {
		doc, err = mongokit.Project(doc, projection)
		if err != nil {
			return &SingleResult{err: err}
		}
	}

	return &SingleResult{doc: doc}
}

// Indexes implements the ICollection.Indexes method.
func (c *Collection) Indexes() IIndexView {
	return &IndexView{
		handle: c.handle,
		engine: c.engine,
	}
}

// InsertMany implements the ICollection.InsertMany method.
func (c *Collection) InsertMany(
	ctx context.Context,
	documents any,
	opts ...options.Lister[options.InsertManyOptions],
) (*mongo.InsertManyResult, error) {
	// merge options
	args, err := NewOptions[options.InsertManyOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"Ordered": supported,
	})

	// check documents
	if documents == nil {
		panic("lungo: missing documents")
	}

	// ensure documents is a slice or array
	rv := reflect.ValueOf(documents)
	kind := rv.Kind()
	if kind != reflect.Slice && kind != reflect.Array {
		panic("lungo: expected slice of documents")
	}
	if rv.Len() == 0 {
		panic("lungo: missing documents")
	}

	// prepare list
	list := make(bsonkit.List, 0, rv.Len())

	// transform documents
	for i := 0; i < rv.Len(); i++ {
		document := rv.Index(i).Interface()
		// transform document
		doc, err := bsonkit.Transform(document)
		if err != nil {
			return nil, err
		}

		// add to list
		list = append(list, doc)
	}

	// get ordered (default true, as in the official driver)
	ordered := true
	if args.Ordered != nil {
		ordered = *args.Ordered
	}

	// insert documents
	result, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Insert(c.handle, list, ordered)
	})
	if err != nil {
		return nil, err
	}

	return &mongo.InsertManyResult{
		InsertedIDs: bsonkit.Pick(result.Modified, "_id", false),
	}, result.Error
}

// InsertOne implements the ICollection.InsertOne method.
func (c *Collection) InsertOne(ctx context.Context, document any,
	opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, error) {
	// merge options
	args, err := NewOptions[options.InsertOneOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{})

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
	result, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Insert(c.handle, bsonkit.List{doc}, true)
	})
	if err != nil {
		return nil, err
	}

	// check error
	if result.Error != nil {
		return nil, result.Error
	}

	return &mongo.InsertOneResult{
		InsertedID: bsonkit.Get(result.Modified[0], "_id"),
	}, nil
}

// Name implements the ICollection.Name method.
func (c *Collection) Name() string {
	return c.handle[1]
}

// ReplaceOne implements the ICollection.ReplaceOne method.
func (c *Collection) ReplaceOne(
	ctx context.Context,
	filter any,
	replacement any,
	opts ...options.Lister[options.ReplaceOptions],
) (*mongo.UpdateResult, error) {
	// merge options
	args, err := NewOptions[options.ReplaceOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
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
	if args.Upsert != nil {
		upsert = *args.Upsert
	}

	// insert document
	result, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Replace(c.handle, query, nil, doc, upsert)
	})
	if err != nil {
		return nil, err
	}

	// check if upserted
	if result.Upserted != nil {
		return &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    bsonkit.Get(result.Upserted, "_id"),
		}, nil
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(len(result.Matched)),
		ModifiedCount: int64(len(result.Modified)),
	}, nil
}

// SearchIndexes implements the ICollection.SearchIndexes method.
func (c *Collection) SearchIndexes() mongo.SearchIndexView {
	panic("lungo: not implemented")
}

// UpdateByID implements the ICollection.UpdateByID method.
func (c *Collection) UpdateByID(
	ctx context.Context,
	id any,
	update any,
	opts ...options.Lister[options.UpdateOneOptions],
) (*mongo.UpdateResult, error) {
	// check id
	if id == nil {
		return nil, mongo.ErrNilValue
	}

	return c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, update, opts...)
}

// UpdateMany implements the ICollection.UpdateMany method.
func (c *Collection) UpdateMany(
	ctx context.Context,
	filter any,
	update any,
	opts ...options.Lister[options.UpdateManyOptions],
) (*mongo.UpdateResult, error) {
	// merge options
	args, err := NewOptions[options.UpdateManyOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"Upsert":       supported,
		"ArrayFilters": supported,
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
	if args.Upsert != nil {
		upsert = *args.Upsert
	}

	// get array filters
	var arrayFilters bsonkit.List
	if args.ArrayFilters != nil {
		arrayFilters, err = bsonkit.TransformList(args.ArrayFilters)
		if err != nil {
			return nil, err
		}
	}

	// update documents
	result, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Update(c.handle, query, nil, doc, 0, 0, upsert, arrayFilters)
	})
	if err != nil {
		return nil, err
	}

	// check if upserted
	if result.Upserted != nil {
		return &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    bsonkit.Get(result.Upserted, "_id"),
		}, nil
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(len(result.Matched)),
		ModifiedCount: int64(len(result.Modified)),
	}, nil
}

// UpdateOne implements the ICollection.UpdateOne method.
func (c *Collection) UpdateOne(
	ctx context.Context,
	filter any,
	update any,
	opts ...options.Lister[options.UpdateOneOptions],
) (*mongo.UpdateResult, error) {
	// merge options
	args, err := NewOptions[options.UpdateOneOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"Upsert":       supported,
		"ArrayFilters": supported,
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
	if args.Upsert != nil {
		upsert = *args.Upsert
	}

	// get array filters
	var arrayFilters bsonkit.List
	if args.ArrayFilters != nil {
		arrayFilters, err = bsonkit.TransformList(args.ArrayFilters)
		if err != nil {
			return nil, err
		}
	}

	// update documents
	result, err := useTransaction(ctx, c.engine, true, func(txn *Transaction) (*Result, error) {
		return txn.Update(c.handle, query, nil, doc, 0, 1, upsert, arrayFilters)
	})
	if err != nil {
		return nil, err
	}

	// check if upserted
	if result.Upserted != nil {
		return &mongo.UpdateResult{
			UpsertedCount: 1,
			UpsertedID:    bsonkit.Get(result.Upserted, "_id"),
		}, nil
	}

	return &mongo.UpdateResult{
		MatchedCount:  int64(len(result.Matched)),
		ModifiedCount: int64(len(result.Modified)),
	}, nil
}

// Watch implements the ICollection.Watch method.
func (c *Collection) Watch(ctx context.Context, pipeline any,
	opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
	// merge options
	args, err := NewOptions[options.ChangeStreamOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"BatchSize":            ignored,
		"FullDocument":         ignored,
		"MaxAwaitTime":         ignored,
		"ResumeAfter":          supported,
		"StartAtOperationTime": supported,
		"StartAfter":           supported,
	})

	// transform pipeline
	filter, err := bsonkit.TransformList(pipeline)
	if err != nil {
		return nil, err
	}

	// get resume after
	var resumeAfter bsonkit.Doc
	if args.ResumeAfter != nil {
		resumeAfter, err = bsonkit.Transform(args.ResumeAfter)
		if err != nil {
			return nil, err
		}
	}

	// get start after
	var startAfter bsonkit.Doc
	if args.StartAfter != nil {
		startAfter, err = bsonkit.Transform(args.StartAfter)
		if err != nil {
			return nil, err
		}
	}

	// open stream
	stream, err := c.engine.Watch(c.handle, filter, resumeAfter, startAfter, args.StartAtOperationTime)
	if err != nil {
		return nil, err
	}

	return stream, nil
}
