package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Client interface {
	Connect(context.Context) error
	Database(string, ...*options.DatabaseOptions) Database
	Disconnect(context.Context) error
	ListDatabaseNames(context.Context, interface{}, ...*options.ListDatabasesOptions) ([]string, error)
	ListDatabases(context.Context, interface{}, ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error)
	Ping(context.Context, *readpref.ReadPref) error
	StartSession(...*options.SessionOptions) (mongo.Session, error)
	UseSession(context.Context, func(mongo.SessionContext) error) error
	UseSessionWithOptions(context.Context, *options.SessionOptions, func(mongo.SessionContext) error) error
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

type Database interface {
	Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (Cursor, error)
	Client() Client
	Collection(string, ...*options.CollectionOptions) Collection
	Drop(context.Context) error
	ListCollectionNames(context.Context, interface{}, ...*options.ListCollectionsOptions) ([]string, error)
	ListCollections(context.Context, interface{}, ...*options.ListCollectionsOptions) (Cursor, error)
	Name() string
	ReadConcern() *readconcern.ReadConcern
	ReadPreference() *readpref.ReadPref
	RunCommand(context.Context, interface{}, ...*options.RunCmdOptions) *mongo.SingleResult
	RunCommandCursor(context.Context, interface{}, ...*options.RunCmdOptions) (Cursor, error)
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
	WriteConcern() *writeconcern.WriteConcern
}

type Collection interface {
	Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (Cursor, error)
	BulkWrite(context.Context, []mongo.WriteModel, ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	Clone(...*options.CollectionOptions) (Collection, error)
	CountDocuments(context.Context, interface{}, ...*options.CountOptions) (int64, error)
	Database() Database
	DeleteMany(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	DeleteOne(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	Distinct(context.Context, string, interface{}, ...*options.DistinctOptions) ([]interface{}, error)
	Drop(context.Context) error
	EstimatedDocumentCount(context.Context, ...*options.EstimatedDocumentCountOptions) (int64, error)
	Find(context.Context, interface{}, ...*options.FindOptions) (Cursor, error)
	FindOne(context.Context, interface{}, ...*options.FindOneOptions) *mongo.SingleResult
	FindOneAndDelete(context.Context, interface{}, ...*options.FindOneAndDeleteOptions) *mongo.SingleResult
	FindOneAndReplace(context.Context, interface{}, interface{}, ...*options.FindOneAndReplaceOptions) *mongo.SingleResult
	FindOneAndUpdate(context.Context, interface{}, interface{}, ...*options.FindOneAndUpdateOptions) *mongo.SingleResult
	Indexes() mongo.IndexView
	InsertMany(context.Context, []interface{}, ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
	InsertOne(context.Context, interface{}, ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
	Name() string
	ReplaceOne(context.Context, interface{}, interface{}, ...*options.ReplaceOptions) (*mongo.UpdateResult, error)
	UpdateMany(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	UpdateOne(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

type Cursor interface {
	All(context.Context, interface{}) error
	Close(context.Context) error
	Decode(interface{}) error
	Err() error
	ID() int64
	Next(context.Context) bool
}
