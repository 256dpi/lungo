package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/256dpi/lungo/bsonkit"
)

var _ IDatabase = &Database{}

type Database struct {
	name   string
	client *Client
}

func (d *Database) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error) {
	panic("lungo: not implemented")
}

func (d *Database) Client() IClient {
	return d.client
}

func (d *Database) Collection(name string, opts ...*options.CollectionOptions) ICollection {
	// merge options
	opt := options.MergeCollectionOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{})

	return &Collection{
		ns:     d.name + "." + name,
		name:   name,
		db:     d,
		client: d.client,
	}
}

func (d *Database) Drop(context.Context) error {
	// drop all namespaces with database prefix
	err := d.client.engine.Drop(d.name + ".*")
	if err != nil {
		return err
	}

	return nil
}

func (d *Database) ListCollectionNames(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]string, error) {
	// list collections
	res, err := d.ListCollections(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	// convert cursor
	csr := res.(*staticCursor)

	// collect names
	names := make([]string, 0)
	for _, doc := range csr.list {
		names = append(names, bsonkit.Get(doc, "name").(string))
	}

	return names, nil
}

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
	list, err := d.client.engine.ListCollections(d.name, query)
	if err != nil {
		return nil, err
	}

	return &staticCursor{list: list}, nil
}

func (d *Database) Name() string {
	return d.name
}

func (d *Database) ReadConcern() *readconcern.ReadConcern {
	return readconcern.New()
}

func (d *Database) ReadPreference() *readpref.ReadPref {
	return readpref.Primary()
}

func (d *Database) RunCommand(context.Context, interface{}, ...*options.RunCmdOptions) ISingleResult {
	panic("lungo: not implemented")
}

func (d *Database) RunCommandCursor(context.Context, interface{}, ...*options.RunCmdOptions) (ICursor, error) {
	panic("lungo: not implemented")
}

func (d *Database) Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	panic("lungo: not implemented")
}

func (d *Database) WriteConcern() *writeconcern.WriteConcern {
	return nil
}
