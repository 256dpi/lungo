package lungo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/256dpi/lungo/bsonkit"
)

// ErrFileNotFound is returned if the specified file was not found in the bucket.
// The value is the same as mongo.ErrFileNotFound and can be used interchangeably.
var ErrFileNotFound = mongo.ErrFileNotFound

// ErrWrongSize is returned if a chunk has the wrong size during download.
// The value is the same as mongo.ErrWrongSize and can be used interchangeably.
var ErrWrongSize = mongo.ErrWrongSize

// UploadBufferSize is the size in bytes of one stream batch. Chunks will be
// written to the db after the sum of chunk lengths is greater than or equal
// to this number.
const UploadBufferSize = 16 * 1024 * 1024 // 16 MiB

// ErrNegativePosition is returned if the resulting position after a seek
// operation is negative.
var ErrNegativePosition = errors.New("negative position")

// ErrUploadInProgress is returned by Delete on a tracked bucket when a marker
// for an in-progress upload of the targeted file exists.
var ErrUploadInProgress = errors.New("upload in progress")

// ErrWrongIndex is returned during download if a chunk's index does not match
// the expected sequential position.
var ErrWrongIndex = errors.New("wrong index")

// The bucket marker states.
const (
	BucketMarkerStateUploading = "uploading"
	BucketMarkerStateUploaded  = "uploaded"
	BucketMarkerStateDeleted   = "deleted"
)

// BucketMarker represents a document stored in the bucket "markers" collection.
type BucketMarker struct {
	ID        bson.ObjectID `bson:"_id"`
	File      interface{}   `bson:"files_id"`
	State     string        `bson:"state"`
	Timestamp time.Time     `bson:"timestamp"`
	Length    int           `bson:"length"`
	ChunkSize int           `bson:"chunkSize"`
	Filename  string        `bson:"filename"`
	Metadata  interface{}   `bson:"metadata,omitempty"`
}

// BucketFile represents a document stored in the bucket "files" collection.
type BucketFile struct {
	ID         interface{} `bson:"_id"`
	Length     int         `bson:"length"`
	ChunkSize  int         `bson:"chunkSize"`
	UploadDate time.Time   `bson:"uploadDate"`
	Filename   string      `bson:"filename"`
	Metadata   interface{} `bson:"metadata,omitempty"`
}

// BucketChunk represents a document stored in the bucket "chunks" collection.
type BucketChunk struct {
	ID   bson.ObjectID `bson:"_id"`
	File interface{}   `bson:"files_id"`
	Num  int           `bson:"n"`
	Data []byte        `bson:"data"`
}

// Bucket provides access to a GridFS bucket. The type is generally compatible
// with gridfs.Bucket from the official driver but allows the passing in of a
// context on all methods. This way the bucket theoretically supports multi-
// document transactions. However, it is not recommended to use transactions for
// large uploads and instead enable the tracking mode and claim the uploads
// to ensure operational safety.
type Bucket struct {
	tracked      bool
	files        ICollection
	chunks       ICollection
	markers      ICollection
	chunkSize    int
	indexMutex   sync.Mutex
	indexEnsured bool
}

// NewBucket creates a bucket using the provided database and options.
func NewBucket(db IDatabase, opts ...options.Lister[options.BucketOptions]) *Bucket {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"Name":           supported,
		"ChunkSizeBytes": supported,
		"WriteConcern":   supported,
		"ReadConcern":    supported,
		"ReadPreference": supported,
	})

	// get name
	name := options.DefaultName
	if opt.Name != nil {
		name = *opt.Name
	}

	// get chunk size
	var chunkSize = int(options.DefaultChunkSize)
	if opt.ChunkSizeBytes != nil {
		chunkSize = int(*opt.ChunkSizeBytes)
	}

	// prepare collection options
	var collOpt = options.Collection().
		SetWriteConcern(opt.WriteConcern).
		SetReadConcern(opt.ReadConcern).
		SetReadPreference(opt.ReadPreference)

	return &Bucket{
		files:     db.Collection(name+".files", collOpt),
		chunks:    db.Collection(name+".chunks", collOpt),
		markers:   db.Collection(name+".markers", collOpt),
		chunkSize: chunkSize,
	}
}

// GetFilesCollection returns the collection used for storing files.
func (b *Bucket) GetFilesCollection(_ context.Context) ICollection {
	return b.files
}

// GetChunksCollection returns the collection used for storing chunks.
func (b *Bucket) GetChunksCollection(_ context.Context) ICollection {
	return b.chunks
}

// GetMarkersCollection returns the collection used for storing markers.
func (b *Bucket) GetMarkersCollection(_ context.Context) ICollection {
	return b.markers
}

// EnableTracking will enable a non-standard mode in which in-progress uploads
// and deletions are tracked by storing a document in an additional "markers"
// collection. If enabled, uploads can be suspended and resumed later and must
// be explicitly claimed. All unclaimed uploads and not fully deleted files
// can be cleaned up.
func (b *Bucket) EnableTracking() {
	b.tracked = true
}

// Delete will remove the specified file from the bucket. If the bucket is
// tracked, only a marker is inserted that will ensure the file and its chunks
// are deleted during the next cleanup. If a marker for an in-progress upload
// of the file exists, ErrUploadInProgress is returned and the upload must be
// aborted before the file can be deleted.
func (b *Bucket) Delete(ctx context.Context, id interface{}) error {
	// in tracked mode, transition or insert a marker without clobbering an
	// existing in-progress upload's marker
	if b.tracked {
		// look up an existing marker for this files_id
		var existing BucketMarker
		err := b.markers.FindOne(ctx, bson.M{"files_id": id}).Decode(&existing)
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}

		// no marker yet: insert a fresh deleted marker
		if err == mongo.ErrNoDocuments {
			_, err = b.markers.InsertOne(ctx, &BucketMarker{
				ID:        bson.NewObjectID(),
				File:      id,
				State:     BucketMarkerStateDeleted,
				Timestamp: time.Now(),
			})
			return err
		}

		// refuse if an upload is in progress; otherwise UploadStream.Close
		// would either fail to find its marker or restore it to "uploaded"
		if existing.State == BucketMarkerStateUploading {
			return ErrUploadInProgress
		}

		// transition the existing marker to deleted, preserving _id so that
		// concurrent claims and cleanup pass continue to address it consistently
		_, err = b.markers.ReplaceOne(ctx, bson.M{"_id": existing.ID}, &BucketMarker{
			ID:        existing.ID,
			File:      id,
			State:     BucketMarkerStateDeleted,
			Timestamp: time.Now(),
		})
		return err
	}

	// delete file
	res1, err := b.files.DeleteOne(ctx, bson.M{
		"_id": id,
	})
	if err != nil {
		return err
	}

	// delete chunks, even if file is missing (orphan chunks are still cleaned
	// up so the namespace doesn't accumulate dangling data)
	_, err = b.chunks.DeleteMany(ctx, bson.M{
		"files_id": id,
	})
	if err != nil {
		return err
	}

	// match the official driver: ErrFileNotFound is reported whenever the
	// files row is missing, regardless of whether orphan chunks existed
	if res1.DeletedCount == 0 {
		return ErrFileNotFound
	}

	return nil
}

// DownloadToStream will download the file with the specified id and write its
// contents to the provided writer.
func (b *Bucket) DownloadToStream(ctx context.Context, id interface{}, w io.Writer) (int64, error) {
	// open stream
	stream, err := b.OpenDownloadStream(ctx, id)
	if err != nil {
		return 0, err
	}

	// copy data
	n, err := io.Copy(w, stream)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// DownloadToStreamByName will download the file with the specified name and
// write its contents to the provided writer.
func (b *Bucket) DownloadToStreamByName(ctx context.Context, name string, w io.Writer, opts ...options.Lister[options.GridFSNameOptions]) (int64, error) {
	// open stream
	stream, err := b.OpenDownloadStreamByName(ctx, name, opts...)
	if err != nil {
		return 0, err
	}

	// copy data
	n, err := io.Copy(w, stream)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// Drop will drop the files and chunks collection. If the bucket is tracked, the
// marker collection is also dropped.
func (b *Bucket) Drop(ctx context.Context) error {
	// drop files
	err := b.files.Drop(ctx)
	if err != nil {
		return err
	}

	// drop chunks
	err = b.chunks.Drop(ctx)
	if err != nil {
		return err
	}

	// drop markers if bucket is tracked
	if b.tracked {
		err = b.markers.Drop(ctx)
		if err != nil {
			return err
		}
	}

	// reset flag
	b.indexMutex.Lock()
	b.indexEnsured = false
	b.indexMutex.Unlock()

	return nil
}

// Find will perform a query on the underlying file collection.
func (b *Bucket) Find(ctx context.Context, filter interface{}, opts ...options.Lister[options.GridFSFindOptions]) (ICursor, error) {
	// merge options
	opt := mergeOptions(opts...)

	// options are asserted by find method

	// prepare find options
	find := options.Find()
	if opt.BatchSize != nil {
		find.SetBatchSize(*opt.BatchSize)
	}
	if opt.Limit != nil {
		find.SetLimit(int64(*opt.Limit))
	}
	if opt.NoCursorTimeout != nil {
		find.SetNoCursorTimeout(*opt.NoCursorTimeout)
	}
	if opt.Skip != nil {
		find.SetSkip(int64(*opt.Skip))
	}
	if opt.Sort != nil {
		find.SetSort(opt.Sort)
	}

	// find files
	csr, err := b.files.Find(ctx, filter, find)
	if err != nil {
		return nil, err
	}

	return csr, nil
}

// OpenDownloadStream will open a download stream for the file with the
// specified id.
func (b *Bucket) OpenDownloadStream(ctx context.Context, id interface{}) (*DownloadStream, error) {
	// create stream
	stream := newDownloadStream(ctx, b, id, "", -1)

	// match the official driver: surface ErrFileNotFound from Open rather
	// than deferring it to the first Read/Seek
	if err := stream.load(); err != nil {
		return nil, err
	}

	return stream, nil
}

// OpenDownloadStreamByName will open a download stream for the file with the
// specified name.
func (b *Bucket) OpenDownloadStreamByName(ctx context.Context, name string, opts ...options.Lister[options.GridFSNameOptions]) (*DownloadStream, error) {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"Revision": supported,
	})

	// get revision
	revision := int(options.DefaultRevision)
	if opt.Revision != nil {
		revision = int(*opt.Revision)
	}

	// create stream
	stream := newDownloadStream(ctx, b, nil, name, revision)

	// match the official driver: surface ErrFileNotFound from Open rather
	// than deferring it to the first Read/Seek
	if err := stream.load(); err != nil {
		return nil, err
	}

	return stream, nil
}

// OpenUploadStream will open an upload stream for a new file with the provided
// name.
func (b *Bucket) OpenUploadStream(ctx context.Context, name string, opts ...options.Lister[options.GridFSUploadOptions]) (*UploadStream, error) {
	return b.OpenUploadStreamWithID(ctx, bson.NewObjectID(), name, opts...)
}

// OpenUploadStreamWithID will open an upload stream for a new file with the
// provided id and name.
func (b *Bucket) OpenUploadStreamWithID(ctx context.Context, id interface{}, name string, opts ...options.Lister[options.GridFSUploadOptions]) (*UploadStream, error) {
	// merge options
	opt := mergeOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"ChunkSizeBytes": supported,
		"Metadata":       supported,
		"Registry":       ignored,
	})

	// ensure indexes
	err := b.EnsureIndexes(ctx, false)
	if err != nil {
		return nil, err
	}

	// get chunk size
	chunkSize := b.chunkSize
	if opt.ChunkSizeBytes != nil {
		chunkSize = int(*opt.ChunkSizeBytes)
	}

	// create stream
	stream := newUploadStream(ctx, b, id, name, chunkSize, opt.Metadata)

	return stream, nil
}

// Rename will rename the file with the specified id to the provided name.
func (b *Bucket) Rename(ctx context.Context, id interface{}, name string) error {
	// rename file
	res, err := b.files.UpdateOne(ctx, bson.M{
		"_id": id,
	}, bson.M{
		"$set": bson.M{
			"filename": name,
		},
	})
	if err != nil {
		return err
	}

	// check result
	if res.MatchedCount == 0 {
		return ErrFileNotFound
	}

	return nil
}

// UploadFromStream will upload a new file using the contents read from the
// provided reader.
func (b *Bucket) UploadFromStream(ctx context.Context, name string, r io.Reader, opts ...options.Lister[options.GridFSUploadOptions]) (bson.ObjectID, error) {
	// prepare id
	id := bson.NewObjectID()

	// upload from stream
	err := b.UploadFromStreamWithID(ctx, id, name, r, opts...)
	if err != nil {
		return bson.ObjectID{}, err
	}

	return id, nil
}

// UploadFromStreamWithID will upload a new file using the contents read from
// the provided reader.
func (b *Bucket) UploadFromStreamWithID(ctx context.Context, id interface{}, name string, r io.Reader, opts ...options.Lister[options.GridFSUploadOptions]) error {
	// open stream
	stream, err := b.OpenUploadStreamWithID(ctx, id, name, opts...)
	if err != nil {
		return err
	}

	// copy data
	_, err = io.Copy(stream, r)
	if err != nil {
		_ = stream.Abort()
		return err
	}

	// close stream
	err = stream.Close()
	if err != nil {
		return err
	}

	return nil
}

// ClaimUpload will claim a tracked upload by creating the file and removing
// the marker.
func (b *Bucket) ClaimUpload(ctx context.Context, id interface{}) error {
	// check if tracked
	if !b.tracked {
		return fmt.Errorf("bucket not tracked")
	}

	// get marker
	var marker BucketMarker
	err := b.markers.FindOne(ctx, bson.M{
		"files_id": id,
	}).Decode(&marker)
	if err != nil {
		return err
	}

	// check marker
	if marker.State != BucketMarkerStateUploaded {
		return fmt.Errorf("upload is not finished")
	}

	// create file
	_, err = b.files.InsertOne(ctx, BucketFile{
		ID:         id,
		Length:     marker.Length,
		ChunkSize:  marker.ChunkSize,
		UploadDate: time.Now(),
		Filename:   marker.Filename,
		Metadata:   marker.Metadata,
	})
	if err != nil {
		return err
	}

	// remove upload marker
	_, err = b.markers.DeleteOne(ctx, bson.M{
		"_id": marker.ID,
	})
	if err != nil {
		return err
	}

	return nil
}

// Cleanup will remove unfinished uploads older than the specified age and all
// files marked for deletion.
func (b *Bucket) Cleanup(ctx context.Context, age time.Duration) error {
	// check if tracked
	if !b.tracked {
		return fmt.Errorf("bucket not tracked")
	}

	// get cursor for old uploads and delete markers
	csr, err := b.markers.Find(ctx, bson.M{
		"$or": []bson.M{
			{
				"state": bson.M{
					"$in": bson.A{BucketMarkerStateUploading, BucketMarkerStateUploaded},
				},
				"timestamp": bson.M{
					"$lt": time.Now().Add(-age),
				},
			},
			{
				"state": BucketMarkerStateDeleted,
			},
		},
	})
	if err != nil {
		return err
	}
	defer csr.Close(ctx)

	// iterate over cursor
	for csr.Next(ctx) {
		// decode marker
		var marker BucketMarker
		err = csr.Decode(&marker)
		if err != nil {
			return err
		}

		// flag marker as deleted if not already
		if marker.State != BucketMarkerStateDeleted {
			res, err := b.markers.UpdateOne(ctx, bson.M{
				"_id":   marker.ID,
				"state": marker.State,
			}, bson.M{
				"$set": bson.M{
					"state": BucketMarkerStateDeleted,
				},
			})
			if err != nil {
				return err
			}

			// continue if marker has been claimed
			if res.ModifiedCount == 0 {
				continue
			}
		}

		// delete file
		_, err := b.files.DeleteOne(ctx, bson.M{
			"_id": marker.File,
		})
		if err != nil {
			return err
		}

		// delete chunks
		_, err = b.chunks.DeleteMany(ctx, bson.M{
			"files_id": marker.File,
		})
		if err != nil {
			return err
		}

		// delete marker
		_, err = b.markers.DeleteOne(ctx, bson.M{
			"_id": marker.ID,
		})
		if err != nil {
			return err
		}
	}

	// check error
	err = csr.Err()
	if err != nil {
		return err
	}

	return nil
}

// EnsureIndexes will check if all required indexes exist and create them when
// needed. Usually, this is done automatically when uploading the first file
// using a bucket. However, when transactions are used to upload files, the
// indexes must be created before the first upload as index creation is
// prohibited during transactions.
func (b *Bucket) EnsureIndexes(ctx context.Context, force bool) error {
	// acquire mutex
	b.indexMutex.Lock()
	defer b.indexMutex.Unlock()

	// return if indexes have been ensured
	if b.indexEnsured {
		return nil
	}

	// clone collection with primary read preference
	files := b.files.Clone(options.Collection().SetReadPreference(readpref.Primary()))

	// unless force is specified, skip index ensuring if files exists already
	var err error
	if !force {
		err = files.FindOne(ctx, bson.M{}).Err()
		if err != nil && err != ErrNoDocuments {
			return err
		} else if err == nil {
			b.indexEnsured = true
			return nil
		}
	}

	// prepare files index
	filesIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: "filename", Value: 1},
			{Key: "uploadDate", Value: 1},
		},
	}

	// prepare chunks index
	chunksIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: "files_id", Value: 1},
			{Key: "n", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}

	// prepare markers index
	markersIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: "files_id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}

	// check files index existence
	hasFilesIndex, err := b.hasIndex(ctx, b.files, filesIndex)
	if err != nil {
		return err
	}

	// check chunks index existence
	hasChunksIndex, err := b.hasIndex(ctx, b.chunks, chunksIndex)
	if err != nil {
		return err
	}

	// check markers index existence
	hasMarkersIndex, err := b.hasIndex(ctx, b.markers, markersIndex)
	if err != nil {
		return err
	}

	// create files index if missing
	if !hasFilesIndex {
		_, err = b.files.Indexes().CreateOne(ctx, filesIndex)
		if err != nil {
			return err
		}
	}

	// create chunks index if missing
	if !hasChunksIndex {
		_, err = b.chunks.Indexes().CreateOne(ctx, chunksIndex)
		if err != nil {
			return err
		}
	}

	// create markers index if missing
	if !hasMarkersIndex {
		_, err = b.markers.Indexes().CreateOne(ctx, markersIndex)
		if err != nil {
			return err
		}
	}

	// set flag
	b.indexEnsured = true

	return nil
}

func (b *Bucket) hasIndex(ctx context.Context, coll ICollection, model mongo.IndexModel) (bool, error) {
	// get indexes
	var indexes []mongo.IndexModel
	csr, err := coll.Indexes().List(ctx)
	if err != nil {
		return false, err
	}
	err = csr.All(nil, &indexes)
	if err != nil {
		return false, err
	}

	// check if index with same keys already exists
	for _, index := range indexes {
		if bsonkit.Compare(index.Keys, model.Keys) == 0 {
			return true, nil
		}
	}

	return false, nil
}

// UploadStream is used to upload a single file.
type UploadStream struct {
	context   context.Context
	bucket    *Bucket
	id        interface{}
	name      string
	metadata  interface{}
	chunkSize int
	marker    *BucketMarker
	length    int
	chunks    int
	buffer    []byte
	bufLen    int
	closed    bool
	mutex     sync.Mutex
}

func newUploadStream(ctx context.Context, bucket *Bucket, id interface{}, name string, chunkSize int, metadata interface{}) *UploadStream {
	return &UploadStream{
		context:   ctx,
		bucket:    bucket,
		id:        id,
		name:      name,
		metadata:  metadata,
		chunkSize: chunkSize,
		buffer:    make([]byte, UploadBufferSize),
	}
}

// Resume will try to resume a previous tracked upload that has been suspended.
// It will return the amount of bytes that have already been written.
func (s *UploadStream) Resume() (int64, error) {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if tracked
	if !s.bucket.tracked {
		return 0, fmt.Errorf("bucket not tracked")
	}

	// check if pristine
	if s.marker != nil || s.bufLen > 0 {
		return 0, fmt.Errorf("stream not pristine")
	}

	// get marker
	err := s.bucket.markers.FindOne(s.context, bson.M{
		"files_id": s.id,
	}).Decode(&s.marker)
	if err != nil {
		return 0, err
	}

	// check marker
	if s.marker.State != BucketMarkerStateUploading {
		return 0, fmt.Errorf("invalid marker state")
	}

	// check marker chunk size
	if s.marker.ChunkSize != s.chunkSize {
		return 0, fmt.Errorf("marker chunk size does not match")
	}

	// create cursor
	csr, err := s.bucket.chunks.Find(s.context, bson.M{
		"files_id": s.id,
	}, options.Find().SetSort(bson.M{
		"n": 1,
	}))
	if err != nil {
		return 0, err
	}

	// ensure cursor is closed
	defer csr.Close(s.context)

	// prepare counters
	var expected int
	var length int

	// check all chunks
	for csr.Next(s.context) {
		// decode chunk
		var chunk BucketChunk
		err = csr.Decode(&chunk)
		if err != nil {
			return 0, err
		}

		// check chunk
		if chunk.Num != expected || len(chunk.Data) != s.chunkSize {
			return 0, fmt.Errorf("found invalid chunk")
		}

		// advance to the next expected chunk number
		expected++
		length += len(chunk.Data)
	}

	// check error
	err = csr.Err()
	if err != nil {
		return 0, err
	}

	// set state (expected equals the count of valid chunks seen)
	s.chunks = expected
	s.length = length

	return int64(length), nil
}

// Abort will abort the upload and remove uploaded chunks. If the bucket is
// tracked it will also remove the potentially created marker. If the abort
// fails the upload may get cleaned up.
func (s *UploadStream) Abort() error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if stream has been closed
	if s.closed {
		return mongo.ErrStreamClosed
	}

	// delete uploaded chunks
	if s.chunks > 0 {
		_, err := s.bucket.chunks.DeleteMany(s.context, bson.M{
			"files_id": s.id,
		})
		if err != nil {
			return err
		}
	}

	// delete marker if it exists
	if s.marker != nil {
		_, err := s.bucket.markers.DeleteOne(s.context, bson.M{
			"_id": s.marker.ID,
		})
		if err != nil {
			return err
		}
	}

	// set flag
	s.closed = true

	return nil
}

// Suspend will upload fully buffered chunks and close the stream. The stream
// may be reopened and resumed later to finish the upload. Until that happens
// the upload may be cleaned up.
func (s *UploadStream) Suspend() (int64, error) {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if tracked
	if !s.bucket.tracked {
		return 0, fmt.Errorf("bucket not tracked")
	}

	// check if stream has been closed
	if s.closed {
		return 0, mongo.ErrStreamClosed
	}

	// upload buffered data
	if s.bufLen > 0 {
		err := s.upload(false)
		if err != nil {
			return 0, err
		}
	}

	// set flag
	s.closed = true

	return int64(s.length), nil
}

// Close will finish the upload and close the stream. If the bucket is tracked
// the method will not finalize the upload by creating a file. Instead, the user
// should call ClaimUpload as part of a multi-document transaction to safely
// claim the upload. Until that happens the upload may be cleaned up.
func (s *UploadStream) Close() error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if stream has been closed
	if s.closed {
		return mongo.ErrStreamClosed
	}

	// upload buffered data; also runs in tracked mode with an empty buffer to
	// ensure a marker has been created
	if s.bufLen > 0 || (s.bucket.tracked && s.marker == nil) {
		err := s.upload(true)
		if err != nil {
			return err
		}
	}

	// update marker if bucket is tracked
	if s.bucket.tracked {
		res, err := s.bucket.markers.ReplaceOne(s.context, bson.M{
			"_id": s.marker.ID,
		}, &BucketMarker{
			ID:        s.marker.ID,
			File:      s.id,
			State:     BucketMarkerStateUploaded,
			Timestamp: time.Now(),
			Length:    s.length,
			ChunkSize: s.chunkSize,
			Filename:  s.name,
			Metadata:  s.metadata,
		})
		if err != nil {
			return err
		} else if res.ModifiedCount != 1 {
			return fmt.Errorf("unable to update marker")
		}
	}

	// otherwise, create file directly
	if !s.bucket.tracked {
		_, err := s.bucket.files.InsertOne(s.context, BucketFile{
			ID:         s.id,
			Length:     s.length,
			ChunkSize:  s.chunkSize,
			UploadDate: time.Now(),
			Filename:   s.name,
			Metadata:   s.metadata,
		})
		if err != nil {
			return err
		}
	}

	// set flag
	s.closed = true

	return nil
}

// Write will write the provided data to chunks in the background. If the bucket
// is tracked and an upload already exists, it must be resumed before writing
// more data.
func (s *UploadStream) Write(data []uint8) (int, error) {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if stream has been closed
	if s.closed {
		return 0, mongo.ErrStreamClosed
	}

	// buffer and upload data in chunks
	var written int
	for {
		// check if done
		if len(data) == 0 {
			break
		}

		// fill buffer
		n := copy(s.buffer[s.bufLen:], data)
		s.bufLen += n

		// resize data
		data = data[n:]

		// increment counter
		written += n

		// upload if buffer is full
		if s.bufLen == len(s.buffer) {
			err := s.upload(false)
			if err != nil {
				return 0, err
			}
		}
	}

	return written, nil
}

func (s *UploadStream) upload(final bool) error {
	// prepare chunks
	chunks := make([]interface{}, 0, (s.bufLen/s.chunkSize)+1)

	// split buffer into chunks
	var chunkedBytes int
	for i := 0; i < s.bufLen; i += s.chunkSize {
		// get chunk size
		size := s.bufLen - i
		if size > s.chunkSize {
			size = s.chunkSize
		}

		// skip partial chunks if not final
		if size < s.chunkSize && !final {
			break
		}

		// append chunk
		chunks = append(chunks, BucketChunk{
			ID:   bson.NewObjectID(),
			File: s.id,
			Num:  s.chunks + len(chunks),
			Data: s.buffer[i : i+size],
		})

		// update counter
		chunkedBytes += size
	}

	// insert upload marker before first write if tracked
	if s.marker == nil && s.bucket.tracked {
		// prepare marker
		s.marker = &BucketMarker{
			ID:        bson.NewObjectID(),
			File:      s.id,
			State:     BucketMarkerStateUploading,
			Timestamp: time.Now(),
			ChunkSize: s.chunkSize,
			Filename:  s.name,
			Metadata:  s.metadata,
		}

		// insert marker
		_, err := s.bucket.markers.InsertOne(s.context, s.marker)
		if err != nil {
			return err
		}
	}

	// write chunks (skip when there is nothing to flush, e.g. close on an
	// empty stream or a partial buffer that should be retained)
	if len(chunks) > 0 {
		_, err := s.bucket.chunks.InsertMany(s.context, chunks)
		if err != nil {
			return err
		}
	}

	// get remaining bytes
	remainingBytes := s.bufLen - chunkedBytes

	// move remaining bytes
	if remainingBytes > 0 {
		copy(s.buffer[0:], s.buffer[chunkedBytes:chunkedBytes+remainingBytes])
	}

	// reset buffer length
	s.bufLen = remainingBytes

	// increment chunk counter
	s.chunks += len(chunks)

	// update file length
	s.length += chunkedBytes

	return nil
}

// DownloadStream is used to download a single file.
type DownloadStream struct {
	context  context.Context
	bucket   *Bucket
	id       interface{}
	name     string
	revision int
	file     *BucketFile
	chunks   int
	position int
	cursor   ICursor
	chunk    *BucketChunk
	buffer   []byte
	closed   bool
	mutex    sync.Mutex
}

func newDownloadStream(ctx context.Context, bucket *Bucket, id interface{}, name string, revision int) *DownloadStream {
	return &DownloadStream{
		context:  ctx,
		bucket:   bucket,
		id:       id,
		name:     name,
		revision: revision,
	}
}

// GetFile will return the file that is stream is downloading from.
func (s *DownloadStream) GetFile() *BucketFile {
	return s.file
}

// Skip will advance the read head by the specified amount of bytes.
func (s *DownloadStream) Skip(skip int64) (int64, error) {
	return s.Seek(skip, io.SeekCurrent)
}

// Seek will reposition the read head using the specified values. A resulting
// position below zero will yield and error while a position beyond the file
// length will yield EOF on subsequent reads.
func (s *DownloadStream) Seek(offset int64, whence int) (int64, error) {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if closed
	if s.closed {
		return 0, mongo.ErrStreamClosed
	}

	// ensure file is loaded
	err := s.load()
	if err != nil {
		return 0, err
	}

	// calculate position
	var position int
	switch whence {
	case io.SeekStart:
		position = int(offset)
	case io.SeekCurrent:
		position = s.position + int(offset)
	case io.SeekEnd:
		position = s.file.Length + int(offset)
	}

	// seek to position
	err = s.seek(position)
	if err != nil {
		return 0, err
	}

	// update position
	s.position = position

	return int64(s.position), nil
}

// Read will read bytes into the specified buffer from the current position of
// the read head.
func (s *DownloadStream) Read(buf []uint8) (int, error) {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if closed
	if s.closed {
		return 0, mongo.ErrStreamClosed
	}

	// ensure file is loaded
	err := s.load()
	if err != nil {
		return 0, err
	}

	// check position
	if s.position >= s.file.Length {
		return 0, io.EOF
	}

	// fill buffer
	read := 0
	for read < len(buf) {
		// check if buffer is empty
		if len(s.buffer) == 0 {
			// get next chunk
			err = s.next()
			if err == io.EOF {
				// only return EOF if no data has been read
				if read == 0 {
					return 0, io.EOF
				}

				return read, nil
			} else if err != nil {
				return read, err
			}
		}

		// copy data
		n := copy(buf[read:], s.buffer)

		// resize buffer
		s.buffer = s.buffer[n:]

		// update position
		s.position += n

		// increment counter
		read += n
	}

	return read, nil
}

// Close will close the download stream.
func (s *DownloadStream) Close() error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if closed
	if s.closed {
		return mongo.ErrStreamClosed
	}

	// close cursor
	if s.cursor != nil {
		err := s.cursor.Close(nil)
		if err != nil {
			return err
		}
	}

	// set flag
	s.closed = true

	return nil
}

func (s *DownloadStream) load() error {
	// skip if file is loaded
	if s.file != nil {
		return nil
	}

	// prepare filter and options to load by id
	filter := bson.M{"_id": s.id}
	opt := options.FindOne()

	// load by name if id is missing
	if s.id == nil {
		// set filter
		filter = bson.M{"filename": s.name}

		// prepare sort and skip
		sort := 1
		skip := s.revision
		if s.revision < 0 {
			sort = -1
			skip = (s.revision * -1) - 1
		}

		// prepare options
		opt = options.FindOne().SetSort(bson.M{
			"uploadDate": sort,
		}).SetSkip(int64(skip))
	}

	// find file
	err := s.bucket.files.FindOne(s.context, filter, opt).Decode(&s.file)
	if err == ErrNoDocuments {
		return ErrFileNotFound
	} else if err != nil {
		return err
	}

	// validate chunk size before integer division
	if s.file.ChunkSize <= 0 {
		return fmt.Errorf("invalid chunk size %d in file metadata", s.file.ChunkSize)
	}

	// set chunks
	s.chunks = s.file.Length / s.file.ChunkSize
	if s.file.Length%s.file.ChunkSize != 0 {
		s.chunks++
	}

	// seek to zero
	err = s.seek(0)
	if err != nil {
		return err
	}

	return nil
}

func (s *DownloadStream) seek(position int) error {
	// check underflow
	if position < 0 {
		return ErrNegativePosition
	}

	// close any previously opened cursor before replacing it
	if s.cursor != nil {
		_ = s.cursor.Close(s.context)
		s.cursor = nil
	}

	// check position
	if position >= s.file.Length {
		s.chunk = nil
		s.buffer = nil
		return nil
	}

	// calculate chunk
	num := position / s.file.ChunkSize

	// create cursor
	cursor, err := s.bucket.chunks.Find(s.context, bson.M{
		"files_id": s.file.ID,
	}, options.Find().SetSort(bson.M{
		"n": 1,
	}).SetSkip(int64(num)))
	if err != nil {
		return err
	}

	// load first chunk
	if !cursor.Next(s.context) {
		// check error
		if cursor.Err() != nil {
			return cursor.Err()
		}

		return fmt.Errorf("expected chunk")
	}

	// decode first chunk
	var chunk BucketChunk
	err = cursor.Decode(&chunk)
	if err != nil {
		return err
	}

	// check chunk
	if chunk.Num != num {
		return ErrWrongIndex
	} else if num < s.chunks-1 && len(chunk.Data) != s.file.ChunkSize {
		return ErrWrongSize
	}

	// set cursor
	s.cursor = cursor

	// set chunk
	s.chunk = &chunk

	// compute offset
	offset := position - (num * s.file.ChunkSize)

	// set buffer
	s.buffer = chunk.Data[offset:]

	return nil
}

func (s *DownloadStream) next() error {
	// no cursor means we are past the end
	if s.cursor == nil {
		return io.EOF
	}

	// advance cursor
	if !s.cursor.Next(s.context) {
		if err := s.cursor.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	// decode next chunk
	var chunk BucketChunk
	err := s.cursor.Decode(&chunk)
	if err != nil {
		return err
	}

	// check chunk
	if chunk.Num != s.chunk.Num+1 {
		return ErrWrongIndex
	} else if chunk.Num < s.chunks-1 && len(chunk.Data) != s.file.ChunkSize {
		return ErrWrongSize
	}

	// set chunk
	s.chunk = &chunk

	// set buffer
	s.buffer = chunk.Data

	return nil
}
