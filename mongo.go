package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ IClient = &MongoClient{}

// MongoClients wraps a mongo.Client to be lungo compatible.
type MongoClient struct {
	*mongo.Client
}

// Connect will connect to a MongoDB database and return a lungo compatible client.
func Connect(ctx context.Context, opts ...*options.ClientOptions) (IClient, error) {
	client, err := mongo.Connect(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &MongoClient{Client: client}, nil
}

// Database implements the IClient.Database method.
func (c *MongoClient) Database(name string, opts ...*options.DatabaseOptions) IDatabase {
	return &MongoDatabase{Database: c.Client.Database(name, opts...), client: c}
}

var _ IDatabase = &MongoDatabase{}

// MongoDatabase wraps a mongo.Database to be lungo compatible.
type MongoDatabase struct {
	*mongo.Database

	client *MongoClient
}

// Aggregate implements the IDatabase.Aggregate method.
func (d *MongoDatabase) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (ICursor, error) {
	return d.Database.Aggregate(ctx, pipeline, opts...)
}

// Client implements the IDatabase.Client method.
func (d *MongoDatabase) Client() IClient {
	return d.client
}

// Collection implements the IDatabase.Collection method.
func (d *MongoDatabase) Collection(name string, opts ...*options.CollectionOptions) ICollection {
	return &MongoCollection{Collection: d.Database.Collection(name, opts...), db: d}
}

// ListCollections implements the IDatabase.ListCollections method.
func (d *MongoDatabase) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (ICursor, error) {
	return d.Database.ListCollections(ctx, filter, opts...)
}

// RunCommand implements the IDatabase.RunCommand method.
func (d *MongoDatabase) RunCommand(ctx context.Context, runCommand interface{}, opts ...*options.RunCmdOptions) ISingleResult {
	return d.Database.RunCommand(ctx, runCommand, opts...)
}

// RunCommandCursor implements the IDatabase.RunCommandCursor method.
func (d *MongoDatabase) RunCommandCursor(ctx context.Context, filter interface{}, opts ...*options.RunCmdOptions) (ICursor, error) {
	return d.Database.RunCommandCursor(ctx, filter, opts...)
}

var _ ICollection = &MongoCollection{}

// MongoCollection wraps a mongo.Collection to be lungo compatible.
type MongoCollection struct {
	*mongo.Collection

	db *MongoDatabase
}

// Aggregate implements the ICollection.Aggregate method.
func (c *MongoCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (ICursor, error) {
	return c.Collection.Aggregate(ctx, pipeline, opts...)
}

// Clone implements the ICollection.Clone method.
func (c *MongoCollection) Clone(opts ...*options.CollectionOptions) (ICollection, error) {
	coll, err := c.Collection.Clone(opts...)
	if err != nil {
		return nil, err
	}

	return &MongoCollection{Collection: coll, db: c.db}, nil
}

// Database implements the ICollection.Database method.
func (c *MongoCollection) Database() IDatabase {
	return c.db
}

// Find implements the ICollection.Find method.
func (c *MongoCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (ICursor, error) {
	return c.Collection.Find(ctx, filter, opts...)
}

// FindOne implements the ICollection.FindOne method.
func (c *MongoCollection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) ISingleResult {
	return c.Collection.FindOne(ctx, filter, opts...)
}

// FindOneAndDelete implements the ICollection.FindOneAndDelete method.
func (c *MongoCollection) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) ISingleResult {
	return c.Collection.FindOneAndDelete(ctx, filter, opts...)
}

// FindOneAndReplace implements the ICollection.FindOneAndReplace method.
func (c *MongoCollection) FindOneAndReplace(ctx context.Context, filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) ISingleResult {
	return c.Collection.FindOneAndReplace(ctx, filter, replacement, opts...)
}

// FindOneAndUpdate implements the ICollection.FindOneAndUpdate method.
func (c *MongoCollection) FindOneAndUpdate(ctx context.Context, filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) ISingleResult {
	return c.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
}

// Indexes implements the ICollection.Indexes method.
func (c *MongoCollection) Indexes() IIndexView {
	i := c.Collection.Indexes()
	return &MongoIndexView{
		IndexView: &i,
	}
}

var _ IIndexView = &MongoIndexView{}

// MongoIndexView wraps a mongo.IndexView to be lungo compatible.
type MongoIndexView struct {
	*mongo.IndexView
}

// List implements the IIndexView.List method.
func (m *MongoIndexView) List(ctx context.Context, opts ...*options.ListIndexesOptions) (ICursor, error) {
	return m.IndexView.List(ctx, opts...)
}
