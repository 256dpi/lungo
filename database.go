package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/256dpi/lungo/bsonkit"
)

var _ IDatabase = &Database{}

// Database wraps an Engine to be mongo compatible.
type Database struct {
	engine *Engine
	name   string
}

// Aggregate implements the IDatabase.Aggregate method.
func (d *Database) Aggregate(context.Context, interface{}, ...options.Lister[options.AggregateOptions]) (ICursor, error) {
	panic("lungo: not implemented")
}

// Client implements the IDatabase.Client method.
func (d *Database) Client() IClient {
	return &Client{
		engine: d.engine,
	}
}

// Collection implements the IDatabase.Collection method.
func (d *Database) Collection(name string, opts ...options.Lister[options.CollectionOptions]) ICollection {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"ReadConcern":    ignored,
		"WriteConcern":   ignored,
		"ReadPreference": ignored,
	})

	return &Collection{
		engine: d.engine,
		handle: Handle{d.name, name},
	}
}

// CreateCollection implements the IDatabase.CreateCollection method.
func (d *Database) CreateCollection(ctx context.Context, name string, opts ...options.Lister[options.CreateCollectionOptions]) error {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

	// begin transaction
	txn, err := d.engine.Begin(ctx, true)
	if err != nil {
		return err
	}

	// ensure abortion
	defer d.engine.Abort(txn)

	// create collection
	err = txn.Create(Handle{d.name, name})
	if err != nil {
		return err
	}

	// commit transaction
	err = d.engine.Commit(txn)
	if err != nil {
		return err
	}

	return nil
}

// CreateView implements the IDatabase.CreateView method.
func (d *Database) CreateView(_ context.Context, _, _ string, _ interface{}, _ ...options.Lister[options.CreateViewOptions]) error {
	panic("lungo: not implemented")
}

// Drop implements the IDatabase.Drop method.
func (d *Database) Drop(ctx context.Context) error {
	// begin transaction
	txn, err := d.engine.Begin(ctx, true)
	if err != nil {
		return err
	}

	// ensure abortion
	defer d.engine.Abort(txn)

	// drop all namespaces with database prefix
	err = txn.Drop(Handle{d.name, ""})
	if err != nil {
		return err
	}

	// commit transaction
	err = d.engine.Commit(txn)
	if err != nil {
		return err
	}

	return nil
}

// ListCollectionNames implements the IDatabase.ListCollectionNames method.
func (d *Database) ListCollectionNames(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListCollectionsOptions]) ([]string, error) {
	// list collections
	res, err := d.ListCollections(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	// convert cursor
	csr := res.(*Cursor)

	// collect names
	names := make([]string, 0)
	for _, doc := range csr.list {
		names = append(names, bsonkit.Get(doc, "name").(string))
	}

	return names, nil
}

// ListCollectionSpecifications implements the
// IDatabase.ListCollectionSpecifications method.
func (d *Database) ListCollectionSpecifications(context.Context, interface{}, ...options.Lister[options.ListCollectionsOptions]) ([]mongo.CollectionSpecification, error) {
	panic("lungo: not implemented")
}

// ListCollections implements the IDatabase.ListCollections method.
func (d *Database) ListCollections(ctx context.Context, filter interface{}, opts ...options.Lister[options.ListCollectionsOptions]) (ICursor, error) {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// list collections (route through useTransaction so session-bound
	// transactions are honored)
	res, err := useTransaction(ctx, d.engine, false, func(txn *Transaction) (interface{}, error) {
		return txn.ListCollections(Handle{d.name}, query)
	})
	if err != nil {
		return nil, err
	}

	return &Cursor{list: res.(bsonkit.List)}, nil
}

// Name implements the IDatabase.Name method.
func (d *Database) Name() string {
	return d.name
}

// RunCommand implements the IDatabase.RunCommand method.
func (d *Database) RunCommand(context.Context, interface{}, ...options.Lister[options.RunCmdOptions]) ISingleResult {
	panic("lungo: not implemented")
}

// RunCommandCursor implements the IDatabase.RunCommandCursor method.
func (d *Database) RunCommandCursor(context.Context, interface{}, ...options.Lister[options.RunCmdOptions]) (ICursor, error) {
	panic("lungo: not implemented")
}

// Watch implements the IDatabase.Watch method.
func (d *Database) Watch(_ context.Context, pipeline interface{}, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"BatchSize":            ignored,
		"Comment":              ignored,
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

	// reject non-empty pipelines
	if len(filter) > 0 {
		panic("lungo: change stream pipelines are not supported")
	}

	// get resume after
	var resumeAfter bsonkit.Doc
	if opt.ResumeAfter != nil {
		resumeAfter, err = bsonkit.Transform(opt.ResumeAfter)
		if err != nil {
			return nil, err
		}
	}

	// get start after
	var startAfter bsonkit.Doc
	if opt.StartAfter != nil {
		startAfter, err = bsonkit.Transform(opt.StartAfter)
		if err != nil {
			return nil, err
		}
	}

	// open stream
	stream, err := d.engine.Watch(Handle{d.name}, filter, resumeAfter, startAfter, opt.StartAtOperationTime)
	if err != nil {
		return nil, err
	}

	return stream, nil
}
