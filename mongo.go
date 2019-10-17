package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ IClient = &MongoClient{}

type MongoClient struct {
	*mongo.Client
}

func Connect(ctx context.Context, opts ...*options.ClientOptions) (IClient, error) {
	client, err := mongo.Connect(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &MongoClient{Client: client}, nil
}

func (c *MongoClient) Database(name string, opts ...*options.DatabaseOptions) IDatabase {
	return &MongoDatabase{Database: c.Client.Database(name, opts...), client: c}
}

var _ IDatabase = &MongoDatabase{}

type MongoDatabase struct {
	*mongo.Database

	client *MongoClient
}

func (d *MongoDatabase) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (ICursor, error) {
	return d.Database.Aggregate(ctx, pipeline, opts...)
}

func (d *MongoDatabase) Client() IClient {
	return d.client
}

func (d *MongoDatabase) Collection(name string, opts ...*options.CollectionOptions) ICollection {
	return &MongoCollection{Collection: d.Database.Collection(name, opts...), db: d}
}

func (d *MongoDatabase) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (ICursor, error) {
	return d.Database.ListCollections(ctx, filter, opts...)
}

func (d *MongoDatabase) RunCommand(ctx context.Context, runCommand interface{}, opts ...*options.RunCmdOptions) ISingleResult {
	return d.Database.RunCommand(ctx, runCommand, opts...)
}

func (d *MongoDatabase) RunCommandCursor(ctx context.Context, filter interface{}, opts ...*options.RunCmdOptions) (ICursor, error) {
	return d.Database.RunCommandCursor(ctx, filter, opts...)
}

var _ ICollection = &MongoCollection{}

type MongoCollection struct {
	*mongo.Collection

	db *MongoDatabase
}

func (c *MongoCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (ICursor, error) {
	return c.Collection.Aggregate(ctx, pipeline, opts...)
}

func (c *MongoCollection) Clone(opts ...*options.CollectionOptions) (ICollection, error) {
	coll, err := c.Collection.Clone(opts...)
	if err != nil {
		return nil, err
	}

	return &MongoCollection{Collection: coll, db: c.db}, nil
}

func (c *MongoCollection) Database() IDatabase {
	return c.db
}

func (c *MongoCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (ICursor, error) {
	return c.Collection.Find(ctx, filter, opts...)
}

func (c *MongoCollection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) ISingleResult {
	return c.Collection.FindOne(ctx, filter, opts...)
}

func (c *MongoCollection) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) ISingleResult {
	return c.Collection.FindOneAndDelete(ctx, filter, opts...)
}

func (c *MongoCollection) FindOneAndReplace(ctx context.Context, filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) ISingleResult {
	return c.Collection.FindOneAndReplace(ctx, filter, replacement, opts...)
}

func (c *MongoCollection) FindOneAndUpdate(ctx context.Context, filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) ISingleResult {
	return c.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
}
