package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/256dpi/lungo/bsonkit"
)

var _ IDatabase = &Database{}

// Database wraps an Engine to be mongo compatible.
type Database struct {
	engine *Engine
	name   string
}

// Aggregate implements the IDatabase.Aggregate method.
func (d *Database) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error) {
	panic("lungo: not implemented")
}

// Client implements the IDatabase.Client method.
func (d *Database) Client() IClient {
	return &Client{
		engine: d.engine,
	}
}

// Collection implements the IDatabase.Collection method.
func (d *Database) Collection(name string, opts ...*options.CollectionOptions) ICollection {
	// merge options
	opt := options.MergeCollectionOptions(opts...)

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

// Drop implements the IDatabase.Drop method.
func (d *Database) Drop(context.Context) error {
	// drop all namespaces with database prefix
	err := d.engine.Drop(Handle{d.name, ""})
	if err != nil {
		return err
	}

	return nil
}

// ListCollectionNames implements the IDatabase.ListCollectionNames method.
func (d *Database) ListCollectionNames(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]string, error) {
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

// ListCollections implements the IDatabase.ListCollections method.
func (d *Database) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (ICursor, error) {
	// merge options
	opt := options.MergeListCollectionsOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// list collections
	list, err := d.engine.ListCollections(Handle{d.name}, query)
	if err != nil {
		return nil, err
	}

	return &Cursor{list: list}, nil
}

// Name implements the IDatabase.Name method.
func (d *Database) Name() string {
	return d.name
}

// ReadConcern implements the IDatabase.ReadConcern method.
func (d *Database) ReadConcern() *readconcern.ReadConcern {
	return readconcern.New()
}

// ReadPreference implements the IDatabase.ReadPreference method.
func (d *Database) ReadPreference() *readpref.ReadPref {
	return readpref.Primary()
}

// RunCommand implements the IDatabase.RunCommand method.
func (d *Database) RunCommand(context.Context, interface{}, ...*options.RunCmdOptions) ISingleResult {
	panic("lungo: not implemented")
}

// RunCommandCursor implements the IDatabase.RunCommandCursor method.
func (d *Database) RunCommandCursor(context.Context, interface{}, ...*options.RunCmdOptions) (ICursor, error) {
	panic("lungo: not implemented")
}

// Watch implements the IDatabase.Watch method.
func (d *Database) Watch(_ context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (IChangeStream, error) {
	// merge options
	opt := options.MergeChangeStreamOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
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

// WriteConcern implements the IDatabase.WriteConcern method.
func (d *Database) WriteConcern() *writeconcern.WriteConcern {
	return nil
}
