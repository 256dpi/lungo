package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type IClient interface {
	Database(string, ...*options.DatabaseOptions) IDatabase
	ListDatabaseNames(context.Context, interface{}, ...*options.ListDatabasesOptions) ([]string, error)
	ListDatabases(context.Context, interface{}, ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error)
	StartSession(...*options.SessionOptions) (mongo.Session, error)
	UseSession(context.Context, func(mongo.SessionContext) error) error
	UseSessionWithOptions(context.Context, *options.SessionOptions, func(mongo.SessionContext) error) error
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

type IDatabase interface {
	Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error)
	Client() IClient
	Collection(string, ...*options.CollectionOptions) ICollection
	Drop(context.Context) error
	ListCollectionNames(context.Context, interface{}, ...*options.ListCollectionsOptions) ([]string, error)
	ListCollections(context.Context, interface{}, ...*options.ListCollectionsOptions) (ICursor, error)
	Name() string
	ReadConcern() *readconcern.ReadConcern
	ReadPreference() *readpref.ReadPref
	RunCommand(context.Context, interface{}, ...*options.RunCmdOptions) ISingleResult
	RunCommandCursor(context.Context, interface{}, ...*options.RunCmdOptions) (ICursor, error)
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
	WriteConcern() *writeconcern.WriteConcern
}

type ICollection interface {
	Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (ICursor, error)
	BulkWrite(context.Context, []mongo.WriteModel, ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	Clone(...*options.CollectionOptions) (ICollection, error)
	CountDocuments(context.Context, interface{}, ...*options.CountOptions) (int64, error)
	Database() IDatabase
	DeleteMany(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	DeleteOne(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	Distinct(context.Context, string, interface{}, ...*options.DistinctOptions) ([]interface{}, error)
	Drop(context.Context) error
	EstimatedDocumentCount(context.Context, ...*options.EstimatedDocumentCountOptions) (int64, error)
	Find(context.Context, interface{}, ...*options.FindOptions) (ICursor, error)
	FindOne(context.Context, interface{}, ...*options.FindOneOptions) ISingleResult
	FindOneAndDelete(context.Context, interface{}, ...*options.FindOneAndDeleteOptions) ISingleResult
	FindOneAndReplace(context.Context, interface{}, interface{}, ...*options.FindOneAndReplaceOptions) ISingleResult
	FindOneAndUpdate(context.Context, interface{}, interface{}, ...*options.FindOneAndUpdateOptions) ISingleResult
	Indexes() mongo.IndexView
	InsertMany(context.Context, []interface{}, ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
	InsertOne(context.Context, interface{}, ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
	Name() string
	ReplaceOne(context.Context, interface{}, interface{}, ...*options.ReplaceOptions) (*mongo.UpdateResult, error)
	UpdateMany(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	UpdateOne(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	Watch(context.Context, interface{}, ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

type ICursor interface {
	All(context.Context, interface{}) error
	Close(context.Context) error
	Decode(interface{}) error
	Err() error
	ID() int64
	Next(context.Context) bool
}

type ISingleResult interface {
	Decode(interface{}) error
	DecodeBytes() (bson.Raw, error)
	Err() error
}
