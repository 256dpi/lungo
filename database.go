package lungo

import (
	"context"

	"github.com/256dpi/lungo/bsonkit"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var _ IDatabase = &Database{}

// Database wraps an Engine to be mongo compatible.
type Database struct {
	engine *Engine
	name   string
}

// Aggregate implements the IDatabase.Aggregate method.
func (d *Database) Aggregate(
	ctx context.Context,
	pipeline any,
	opts ...options.Lister[options.AggregateOptions],
) (ICursor, error) {
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
		engine: d.engine,
		handle: Handle{d.name, name},
	}
}

// CreateCollection implements the IDatabase.CreateCollection method.
func (d *Database) CreateCollection(ctx context.Context, name string, opts ...options.Lister[options.CreateCollectionOptions]) error {
	// merge options
	args, err := NewOptions[options.CreateCollectionOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{})

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
func (d *Database) CreateView(ctx context.Context, viewName, viewOn string, pipeline any, opts ...options.Lister[options.CreateViewOptions]) error {
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
func (d *Database) ListCollectionNames(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.ListCollectionsOptions],
) ([]string, error) {
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
func (d *Database) ListCollectionSpecifications(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.ListCollectionsOptions],
) ([]mongo.CollectionSpecification, error) {
	panic("lungo: not implemented")
}

// ListCollections implements the IDatabase.ListCollections method.
func (d *Database) ListCollections(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.ListCollectionsOptions],
) (ICursor, error) {
	// merge options
	args, err := NewOptions[options.ListCollectionsOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{})

	// transform filter
	query, err := bsonkit.Transform(filter)
	if err != nil {
		return nil, err
	}

	// begin transaction
	txn, err := d.engine.Begin(ctx, false)
	if err != nil {
		return nil, err
	}

	// list collections
	list, err := txn.ListCollections(Handle{d.name}, query)
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
//func (d *Database) ReadConcern() *readconcern.ReadConcern {
//	return readconcern.New()
//}

// ReadPreference implements the IDatabase.ReadPreference method.
//func (d *Database) ReadPreference() *readpref.ReadPref {
//	return readpref.Primary()
//}

// RunCommand implements the IDatabase.RunCommand method.
func (d *Database) RunCommand(
	ctx context.Context,
	runCommand any,
	opts ...options.Lister[options.RunCmdOptions],
) ISingleResult {
	panic("lungo: not implemented")
}

// RunCommandCursor implements the IDatabase.RunCommandCursor method.
func (d *Database) RunCommandCursor(
	ctx context.Context,
	runCommand any,
	opts ...options.Lister[options.RunCmdOptions],
) (ICursor, error) {
	panic("lungo: not implemented")
}

// Watch implements the IDatabase.Watch method.
func (d *Database) Watch(ctx context.Context, pipeline any, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
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
	stream, err := d.engine.Watch(Handle{d.name}, filter, resumeAfter, startAfter, args.StartAtOperationTime)
	if err != nil {
		return nil, err
	}

	return stream, nil
}

// WriteConcern implements the IDatabase.WriteConcern method.
//func (d *Database) WriteConcern() *writeconcern.WriteConcern {
//	return nil
//}

func (d *Database) GridFSBucket(opts ...options.Lister[options.BucketOptions]) IGridFSBucket {
	// merge options
	args, err := NewOptions[options.BucketOptions](opts...)

	if err != nil {
		panic(err)
	}
	// assert supported options
	assertOptions(args, map[string]string{
		"Name":           supported,
		"ChunkSizeBytes": supported,
		"WriteConcern":   supported,
		"ReadConcern":    supported,
		"ReadPreference": supported,
	})

	// get name
	name := options.DefaultName
	if args.Name != nil {
		name = *args.Name
	}

	// get chunk size
	var chunkSize = int(options.DefaultChunkSize)
	if args.ChunkSizeBytes != nil {
		chunkSize = int(*args.ChunkSizeBytes)
	}

	// prepare collection options
	var collOpt = options.Collection().
		SetWriteConcern(args.WriteConcern).
		SetReadConcern(args.ReadConcern).
		SetReadPreference(args.ReadPreference)

	return &Bucket{
		files:     d.Collection(name+".files", collOpt),
		chunks:    d.Collection(name+".chunks", collOpt),
		markers:   d.Collection(name+".markers", collOpt),
		chunkSize: chunkSize,
	}
}
