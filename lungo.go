package lungo

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/session"
)

// IClient defines a generic client.
type IClient interface {
	AppendDriverInfo(info options.DriverInfo)
	BulkWrite(ctx context.Context, writes []mongo.ClientBulkWrite,
		opts ...options.Lister[options.ClientBulkWriteOptions]) (*mongo.ClientBulkWriteResult, error)

	//Connect(context.Context) error
	Database(name string, opts ...options.Lister[options.DatabaseOptions]) IDatabase
	Disconnect(ctx context.Context) error
	ListDatabaseNames(ctx context.Context, filter any, opts ...options.Lister[options.ListDatabasesOptions]) ([]string, error)
	ListDatabases(ctx context.Context, filter any, opts ...options.Lister[options.ListDatabasesOptions]) (mongo.ListDatabasesResult, error)
	NumberSessionsInProgress() int
	Ping(ctx context.Context, rp *readpref.ReadPref) error
	StartSession(opts ...options.Lister[options.SessionOptions]) (ISession, error)
	//Timeout() *time.Duration
	UseSession(ctx context.Context, fn func(context.Context) error) error
	UseSessionWithOptions(ctx context.Context, opts *options.SessionOptionsBuilder, fn func(context.Context) error,
	) error
	Watch(ctx context.Context, pipeline any, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error)
}

// IDatabase defines a generic database.
type IDatabase interface {
	Aggregate(
		ctx context.Context,
		pipeline any,
		opts ...options.Lister[options.AggregateOptions],
	) (ICursor, error)
	Client() IClient
	Collection(name string, opts ...options.Lister[options.CollectionOptions]) ICollection
	CreateCollection(ctx context.Context, name string, opts ...options.Lister[options.CreateCollectionOptions]) error
	CreateView(ctx context.Context, viewName, viewOn string, pipeline any, opts ...options.Lister[options.CreateViewOptions]) error
	Drop(context.Context) error
	ListCollectionNames(
		ctx context.Context,
		filter any,
		opts ...options.Lister[options.ListCollectionsOptions],
	) ([]string, error)
	ListCollectionSpecifications(
		ctx context.Context,
		filter any,
		opts ...options.Lister[options.ListCollectionsOptions],
	) ([]mongo.CollectionSpecification, error)
	ListCollections(
		ctx context.Context,
		filter any,
		opts ...options.Lister[options.ListCollectionsOptions],
	) (ICursor, error)
	Name() string
	//ReadConcern() *readconcern.ReadConcern
	//ReadPreference() *readpref.ReadPref
	RunCommand(
		ctx context.Context,
		runCommand any,
		opts ...options.Lister[options.RunCmdOptions],
	) ISingleResult
	RunCommandCursor(
		ctx context.Context,
		runCommand any,
		opts ...options.Lister[options.RunCmdOptions],
	) (ICursor, error)
	Watch(ctx context.Context, pipeline any, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error)
	//WriteConcern() *writeconcern.WriteConcern
	GridFSBucket(opts ...options.Lister[options.BucketOptions]) IGridFSBucket
}

// ICollection defines a generic collection.
type ICollection interface {
	Aggregate(
		ctx context.Context,
		pipeline any,
		opts ...options.Lister[options.AggregateOptions],
	) (ICursor, error)
	BulkWrite(ctx context.Context, models []mongo.WriteModel,
		opts ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error)
	Clone(opts ...options.Lister[options.CollectionOptions]) ICollection
	CountDocuments(ctx context.Context, filter any,
		opts ...options.Lister[options.CountOptions]) (int64, error)
	Database() IDatabase
	DeleteMany(
		ctx context.Context,
		filter any,
		opts ...options.Lister[options.DeleteManyOptions],
	) (*mongo.DeleteResult, error)
	DeleteOne(
		ctx context.Context,
		filter any,
		opts ...options.Lister[options.DeleteOneOptions],
	) (*mongo.DeleteResult, error)
	Distinct(
		ctx context.Context,
		fieldName string,
		filter any,
		opts ...options.Lister[options.DistinctOptions],
	) IDistinctResult
	Drop(ctx context.Context, opts ...options.Lister[options.DropCollectionOptions]) error
	EstimatedDocumentCount(
		ctx context.Context,
		opts ...options.Lister[options.EstimatedDocumentCountOptions],
	) (int64, error)
	Find(ctx context.Context, filter any,
		opts ...options.Lister[options.FindOptions]) (ICursor, error)
	FindOne(ctx context.Context, filter any,
		opts ...options.Lister[options.FindOneOptions]) ISingleResult
	FindOneAndDelete(
		ctx context.Context,
		filter any,
		opts ...options.Lister[options.FindOneAndDeleteOptions]) ISingleResult
	FindOneAndReplace(
		ctx context.Context,
		filter any,
		replacement any,
		opts ...options.Lister[options.FindOneAndReplaceOptions],
	) ISingleResult
	FindOneAndUpdate(
		ctx context.Context,
		filter any,
		update any,
		opts ...options.Lister[options.FindOneAndUpdateOptions]) ISingleResult
	Indexes() IIndexView
	InsertMany(
		ctx context.Context,
		documents any,
		opts ...options.Lister[options.InsertManyOptions],
	) (*mongo.InsertManyResult, error)
	InsertOne(ctx context.Context, document any,
		opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, error)
	Name() string
	ReplaceOne(
		ctx context.Context,
		filter any,
		replacement any,
		opts ...options.Lister[options.ReplaceOptions],
	) (*mongo.UpdateResult, error)
	SearchIndexes() mongo.SearchIndexView
	UpdateByID(
		ctx context.Context,
		id any,
		update any,
		opts ...options.Lister[options.UpdateOneOptions],
	) (*mongo.UpdateResult, error)
	UpdateMany(
		ctx context.Context,
		filter any,
		update any,
		opts ...options.Lister[options.UpdateManyOptions],
	) (*mongo.UpdateResult, error)
	UpdateOne(
		ctx context.Context,
		filter any,
		update any,
		opts ...options.Lister[options.UpdateOneOptions],
	) (*mongo.UpdateResult, error)
	Watch(ctx context.Context, pipeline any,
		opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error)
}

// ICursor defines a generic cursor.
type ICursor interface {
	All(ctx context.Context, results any) error
	Close(context.Context) error
	Decode(val any) error
	Err() error
	ID() int64
	Next(context.Context) bool
	RemainingBatchLength() int
	SetBatchSize(batchSize int32)
	SetComment(comment any)
	SetMaxAwaitTime(time.Duration)
	TryNext(context.Context) bool
}

// ISingleResult defines a generic single result
type ISingleResult interface {
	Decode(v any) error
	Err() error
	Raw() (bson.Raw, error)
}

// IIndexView defines a generic index view.
type IIndexView interface {
	CreateMany(
		ctx context.Context,
		models []mongo.IndexModel,
		opts ...options.Lister[options.CreateIndexesOptions],
	) ([]string, error)
	CreateOne(
		ctx context.Context,
		model mongo.IndexModel,
		opts ...options.Lister[options.CreateIndexesOptions],
	) (string, error)
	DropAll(
		ctx context.Context,
		opts ...options.Lister[options.DropIndexesOptions],
	) error
	DropOne(
		ctx context.Context,
		name string,
		opts ...options.Lister[options.DropIndexesOptions],
	) error
	DropWithKey(ctx context.Context, keySpecDocument any, opts ...options.Lister[options.DropIndexesOptions]) error
	List(ctx context.Context, opts ...options.Lister[options.ListIndexesOptions]) (ICursor, error)
	ListSpecifications(
		ctx context.Context,
		opts ...options.Lister[options.ListIndexesOptions],
	) ([]mongo.IndexSpecification, error)
}

// IChangeStream defines a generic change stream.
type IChangeStream interface {
	Close(context.Context) error
	Decode(interface{}) error
	Err() error
	ID() int64
	Next(context.Context) bool
	ResumeToken() bson.Raw
	SetBatchSize(int32)
	TryNext(context.Context) bool
	RemainingBatchLength() int
}

// ISession defines a generic session.
type ISession interface {
	ID() bson.Raw
	AbortTransaction(context.Context) error
	AdvanceClusterTime(bson.Raw) error
	AdvanceOperationTime(*bson.Timestamp) error
	Client() IClient
	ClusterTime() bson.Raw
	CommitTransaction(context.Context) error
	EndSession(context.Context)
	OperationTime() *bson.Timestamp
	StartTransaction(...options.Lister[options.TransactionOptions]) error
	WithTransaction(
		context.Context,
		func(ctx context.Context) (any, error),
	...options.Lister[options.TransactionOptions],
	) (any, error)

	ClientSession() *session.Client
}

// ISessionContext defines a generic session context.
type ISessionContext interface {
	context.Context
	ISession
}

// WithSession will yield a session context to the provided callback that uses
// the specified session.
func WithSession(ctx context.Context, session ISession, fn func(ctx context.Context) error) error {
	switch ses := session.(type) {
	case *MongoSession:
		return mongo.WithSession(ensureContext(ctx), ses.Session, fn)
	case *Session:
		return fn(context.WithValue(ensureContext(ctx), sessionKey{}, ses))
	default:
		return fmt.Errorf("unknown session %T", session)
	}
}

func SessionFromContext(ctx context.Context) ISession {
	val := mongo.SessionFromContext(ctx)
	if val != nil {
		return &MongoSession{
			Session: val,
		}
	}
	ctxVal := ctx.Value(sessionKey{})
	sess, ok := ctxVal.(*Session)
	if !ok {
		return nil
	}
	return sess
}

type IGridFSBucket interface {
	OpenUploadStream(
		ctx context.Context,
		filename string,
		opts ...options.Lister[options.GridFSUploadOptions],
	) (IGridFSUploadStream, error)
	OpenUploadStreamWithID(
		ctx context.Context,
		fileID any,
		filename string,
		opts ...options.Lister[options.GridFSUploadOptions],
	) (IGridFSUploadStream, error)
	UploadFromStream(
		ctx context.Context,
		filename string,
		source io.Reader,
		opts ...options.Lister[options.GridFSUploadOptions],
	) (bson.ObjectID, error)
	UploadFromStreamWithID(
		ctx context.Context,
		fileID any,
		filename string,
		source io.Reader,
		opts ...options.Lister[options.GridFSUploadOptions],
	) error
	OpenDownloadStream(ctx context.Context, fileID any) (IGridFSDownloadStream, error)
	DownloadToStream(ctx context.Context, fileID any, stream io.Writer) (int64, error)
	OpenDownloadStreamByName(
		ctx context.Context,
		filename string,
		opts ...options.Lister[options.GridFSNameOptions],
	) (IGridFSDownloadStream, error)
	DownloadToStreamByName(
		ctx context.Context,
		filename string,
		stream io.Writer,
		opts ...options.Lister[options.GridFSNameOptions],
	) (int64, error)
	Delete(ctx context.Context, fileID any) error
	Find(
		ctx context.Context,
		filter any,
		opts ...options.Lister[options.GridFSFindOptions],
	) (ICursor, error)
	Rename(ctx context.Context, fileID any, newFilename string) error
	Drop(ctx context.Context) error
	GetFilesCollection() ICollection
	GetChunksCollection() ICollection
}
type IGridFSDownloadStream interface {
	Close() error
	Read(p []byte) (int, error)
	Skip(skip int64) (int64, error)
	GetFile() IGridFSFile
}
type IGridFSFile interface {
	UnmarshalBSON(data []byte) error
}
type IGridFSUploadStream interface {
	Close() error
	Write(p []byte) (int, error)
	Abort() error
}
type IDistinctResult interface {
	Decode(v any) error
	Err() error
	Raw() (bson.RawArray, error)
}
