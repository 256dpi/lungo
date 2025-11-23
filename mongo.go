package lungo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var _ IClient = &MongoClient{}

// MongoClient wraps a mongo.Client to be lungo compatible.
type MongoClient struct {
	*mongo.Client
}

// Connect will connect to a MongoDB database and return a lungo compatible client.
func Connect(opts ...*options.ClientOptions) (IClient, error) {
	client, err := mongo.Connect(opts...)
	if err != nil {
		return nil, err
	}
	return &MongoClient{Client: client}, nil
}

// Database implements the IClient.Database method.
func (c *MongoClient) Database(name string, opts ...options.Lister[options.DatabaseOptions]) IDatabase {
	return &MongoDatabase{Database: c.Client.Database(name, opts...), client: c}
}

// StartSession implements the IClient.StartSession method.
func (c *MongoClient) StartSession(opts ...options.Lister[options.SessionOptions]) (ISession, error) {
	session, err := c.Client.StartSession(opts...)
	if err != nil {
		return nil, err
	}
	return &MongoSession{Session: session, client: c}, nil
}

// UseSession implements the IClient.UseSession method.
func (c *MongoClient) UseSession(ctx context.Context, fn func(context.Context) error) error {
	return c.UseSessionWithOptions(ctx, options.Session(), fn)
}

// UseSessionWithOptions implements the IClient.UseSessionWithOptions method.
func (c *MongoClient) UseSessionWithOptions(ctx context.Context, opts *options.SessionOptionsBuilder, fn func(context.Context) error) error {
	return c.Client.UseSessionWithOptions(ensureContext(ctx), opts, fn)
}

// Watch implements the IClient.Watch method.
func (c *MongoClient) Watch(ctx context.Context, pipeline any, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
	return c.Client.Watch(ctx, pipeline, opts...)
}

var _ IDatabase = &MongoDatabase{}

// MongoDatabase wraps a mongo.Database to be lungo compatible.
type MongoDatabase struct {
	*mongo.Database

	client *MongoClient
}

// Aggregate implements the IDatabase.Aggregate method.
func (d *MongoDatabase) Aggregate(
	ctx context.Context,
	pipeline any,
	opts ...options.Lister[options.AggregateOptions],
) (ICursor, error) {
	return d.Database.Aggregate(ctx, pipeline, opts...)
}

// Client implements the IDatabase.Client method.
func (d *MongoDatabase) Client() IClient {
	return d.client
}

// Collection implements the IDatabase.Collection method.
func (d *MongoDatabase) Collection(name string, opts ...options.Lister[options.CollectionOptions]) ICollection {
	return &MongoCollection{Collection: d.Database.Collection(name, opts...), db: d}
}

// CreateCollection implements the IDatabase.CreateCollection method.
func (d *MongoDatabase) CreateCollection(ctx context.Context, name string, opts ...options.Lister[options.CreateCollectionOptions]) error {
	return d.Database.CreateCollection(ensureContext(ctx), name, opts...)
}

// ListCollections implements the IDatabase.ListCollections method.
func (d *MongoDatabase) ListCollections(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.ListCollectionsOptions],
) (ICursor, error) {
	return d.Database.ListCollections(ctx, filter, opts...)
}

// RunCommand implements the IDatabase.RunCommand method.
func (d *MongoDatabase) RunCommand(
	ctx context.Context,
	runCommand any,
	opts ...options.Lister[options.RunCmdOptions],
) ISingleResult {
	return d.Database.RunCommand(ctx, runCommand, opts...)
}

// RunCommandCursor implements the IDatabase.RunCommandCursor method.
func (d *MongoDatabase) RunCommandCursor(
	ctx context.Context,
	runCommand any,
	opts ...options.Lister[options.RunCmdOptions],
) (ICursor, error) {
	return d.Database.RunCommandCursor(ctx, runCommand, opts...)
}

// Watch implements the IDatabase.Watch method.
func (d *MongoDatabase) Watch(ctx context.Context, pipeline any, opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
	return d.Database.Watch(ctx, pipeline, opts...)
}
func (d *MongoDatabase) GridFSBucket(opts ...options.Lister[options.BucketOptions]) IGridFSBucket {
	return &MongoGridFSBucket{
		d.Database.GridFSBucket(opts...),
	}
}

var _ IGridFSBucket = &MongoGridFSBucket{}

type MongoGridFSBucket struct {
	*mongo.GridFSBucket
}

func (c *MongoGridFSBucket) OpenUploadStream(
	ctx context.Context,
	filename string,
	opts ...options.Lister[options.GridFSUploadOptions],
) (IGridFSUploadStream, error) {
	return c.GridFSBucket.OpenUploadStream(ctx, filename, opts...)
}

func (c *MongoGridFSBucket) OpenUploadStreamWithID(
	ctx context.Context,
	fileID any,
	filename string,
	opts ...options.Lister[options.GridFSUploadOptions],
) (IGridFSUploadStream, error) {
	return c.GridFSBucket.OpenUploadStreamWithID(ctx, fileID, filename, opts...)
}

func (c *MongoGridFSBucket) OpenDownloadStream(ctx context.Context, fileID any) (IGridFSDownloadStream, error) {
	stream, err := c.GridFSBucket.OpenDownloadStream(ctx, fileID)
	if err != nil {
		return nil, err
	}
	return &MongoGridFSDownloadStream{
		stream,
	}, nil
}

func (c *MongoGridFSBucket) OpenDownloadStreamByName(
	ctx context.Context,
	filename string,
	opts ...options.Lister[options.GridFSNameOptions],
) (IGridFSDownloadStream, error) {
	stream, err := c.GridFSBucket.OpenDownloadStreamByName(ctx, filename, opts...)
	if err != nil {
		return nil, err
	}
	return &MongoGridFSDownloadStream{
		stream,
	}, nil
}

func (c *MongoGridFSBucket) GetFilesCollection() ICollection {
	return &MongoCollection{Collection: c.GridFSBucket.GetFilesCollection()}
}

func (c *MongoGridFSBucket) GetChunksCollection() ICollection {
	return &MongoCollection{Collection: c.GridFSBucket.GetChunksCollection()}
}

func (c *MongoGridFSBucket) Find(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.GridFSFindOptions],
) (ICursor, error) {
	return c.GridFSBucket.Find(ctx, filter, opts...)
}

var _ IGridFSDownloadStream = &MongoGridFSDownloadStream{}

type MongoGridFSDownloadStream struct {
	*mongo.GridFSDownloadStream
}

func (c *MongoGridFSDownloadStream) GetFile() IGridFSFile {
	return c.GridFSDownloadStream.GetFile()
}

var _ ICollection = &MongoCollection{}

// MongoCollection wraps a mongo.Collection to be lungo compatible.
type MongoCollection struct {
	*mongo.Collection

	db *MongoDatabase
}

func (c *MongoCollection) Distinct(
	ctx context.Context,
	fieldName string,
	filter any,
	opts ...options.Lister[options.DistinctOptions],
) IDistinctResult {
	return c.Collection.Distinct(ctx, fieldName, filter, opts...)
}

// Aggregate implements the ICollection.Aggregate method.
func (c *MongoCollection) Aggregate(
	ctx context.Context,
	pipeline any,
	opts ...options.Lister[options.AggregateOptions],
) (ICursor, error) {
	return c.Collection.Aggregate(ctx, pipeline, opts...)
}

// Clone implements the ICollection.Clone method.
func (c *MongoCollection) Clone(opts ...options.Lister[options.CollectionOptions]) ICollection {
	coll := c.Collection.Clone(opts...)
	return &MongoCollection{Collection: coll, db: c.db}
}

// Database implements the ICollection.Database method.
func (c *MongoCollection) Database() IDatabase {
	return c.db
}

// Find implements the ICollection.Find method.
func (c *MongoCollection) Find(ctx context.Context, filter any,
	opts ...options.Lister[options.FindOptions]) (ICursor, error) {
	return c.Collection.Find(ctx, filter, opts...)
}

// FindOne implements the ICollection.FindOne method.
func (c *MongoCollection) FindOne(ctx context.Context, filter any,
	opts ...options.Lister[options.FindOneOptions]) ISingleResult {
	return c.Collection.FindOne(ctx, filter, opts...)
}

// FindOneAndDelete implements the ICollection.FindOneAndDelete method.
func (c *MongoCollection) FindOneAndDelete(
	ctx context.Context,
	filter any,
	opts ...options.Lister[options.FindOneAndDeleteOptions]) ISingleResult {
	return c.Collection.FindOneAndDelete(ctx, filter, opts...)
}

// FindOneAndReplace implements the ICollection.FindOneAndReplace method.
func (c *MongoCollection) FindOneAndReplace(
	ctx context.Context,
	filter any,
	replacement any,
	opts ...options.Lister[options.FindOneAndReplaceOptions],
) ISingleResult {
	return c.Collection.FindOneAndReplace(ctx, filter, replacement, opts...)
}

// FindOneAndUpdate implements the ICollection.FindOneAndUpdate method.
func (c *MongoCollection) FindOneAndUpdate(
	ctx context.Context,
	filter any,
	update any,
	opts ...options.Lister[options.FindOneAndUpdateOptions]) ISingleResult {
	return c.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
}

// Indexes implements the ICollection.Indexes method.
func (c *MongoCollection) Indexes() IIndexView {
	i := c.Collection.Indexes()
	return &MongoIndexView{
		IndexView: &i,
	}
}

// Watch implements the ICollection.Watch method.
func (c *MongoCollection) Watch(ctx context.Context, pipeline any,
	opts ...options.Lister[options.ChangeStreamOptions]) (IChangeStream, error) {
	return c.Collection.Watch(ctx, pipeline, opts...)
}

var _ IIndexView = &MongoIndexView{}

// MongoIndexView wraps a mongo.IndexView to be lungo compatible.
type MongoIndexView struct {
	*mongo.IndexView
}

// CreateMany implements the IIndexView.List method.
func (m *MongoIndexView) CreateMany(
	ctx context.Context,
	models []mongo.IndexModel,
	opts ...options.Lister[options.CreateIndexesOptions],
) ([]string, error) {
	return m.IndexView.CreateMany(ensureContext(ctx), models, opts...)
}

// CreateOne implements the IIndexView.List method.
func (m *MongoIndexView) CreateOne(
	ctx context.Context,
	model mongo.IndexModel,
	opts ...options.Lister[options.CreateIndexesOptions],
) (string, error) {
	return m.IndexView.CreateOne(ensureContext(ctx), model, opts...)
}

// DropAll implements the IIndexView.List method.
func (m *MongoIndexView) DropAll(
	ctx context.Context,
	opts ...options.Lister[options.DropIndexesOptions],
) error {
	return m.IndexView.DropAll(ensureContext(ctx), opts...)
}

// DropOne implements the IIndexView.List method.
func (m *MongoIndexView) DropOne(
	ctx context.Context,
	name string,
	opts ...options.Lister[options.DropIndexesOptions],
) error {
	return m.IndexView.DropOne(ensureContext(ctx), name, opts...)
}

// List implements the IIndexView.List method.
func (m *MongoIndexView) List(ctx context.Context, opts ...options.Lister[options.ListIndexesOptions]) (ICursor, error) {
	return m.IndexView.List(ctx, opts...)
}

var _ ISession = &MongoSession{}

// MongoSession wraps a mongo.Session to be lungo compatible.
type MongoSession struct {
	*mongo.Session

	client *MongoClient
}

// AbortTransaction implements the ISession.Client method.
func (s *MongoSession) AbortTransaction(ctx context.Context) error {
	return s.Session.AbortTransaction(ensureContext(ctx))
}

// Client implements the ISession.Client method.
func (s *MongoSession) Client() IClient {
	return s.client
}

// CommitTransaction implements the ISession.Client method.
func (s *MongoSession) CommitTransaction(ctx context.Context) error {
	return s.Session.CommitTransaction(ensureContext(ctx))
}

// EndSession implements the ISession.Client method.
func (s *MongoSession) EndSession(ctx context.Context) {
	s.Session.EndSession(ensureContext(ctx))
}

// WithTransaction implements the ISession.WithTransaction method.
func (s *MongoSession) WithTransaction(
	ctx context.Context,
	fn func(ctx context.Context) (any, error),
	opts ...options.Lister[options.TransactionOptions],
) (any, error) {
	return s.Session.WithTransaction(ensureContext(ctx), fn, opts...)
}
