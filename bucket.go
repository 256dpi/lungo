package lungo

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/256dpi/lungo/bsonkit"
)

// ErrFileNotFound is returned if the specified file was not found in the bucket.
// The value is the same as gridfs.ErrFileNotFound and can be used interchangeably.
var ErrFileNotFound = gridfs.ErrFileNotFound

// BucketFile represents a document stored in the GridFS files collection.
type BucketFile struct {
	ID         interface{} `bson:"_id"`
	Length     int         `bson:"length"`
	ChunkSize  int         `bson:"chunkSize"`
	UploadDate time.Time   `bson:"uploadDate"`
	Filename   string      `bson:"filename"`
	Metadata   interface{} `bson:"metadata,omitempty"`
}

// BucketChunk represents a document stored in the GridFS chunks collection.
type BucketChunk struct {
	ID   primitive.ObjectID `bson:"_id"`
	File interface{}        `bson:"files_id"`
	Num  int                `bson:"n"`
	Data []byte             `bson:"data"`
}

// Bucket provides access to a GridFS bucket.
type Bucket struct {
	files        ICollection
	chunks       ICollection
	chunkSize    int
	indexMutex   sync.Mutex
	indexEnsured bool
}

// NewBucket creates a bucket using the provided database and options.
func NewBucket(db IDatabase, opts ...*options.BucketOptions) *Bucket {
	// merge options
	opt := options.MergeBucketOptions(opts...)

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
		chunkSize: chunkSize,
	}
}

// Delete will remove the specified file from the bucket.
func (b *Bucket) Delete(ctx context.Context, id interface{}) error {
	// delete file
	res1, err := b.files.DeleteOne(ctx, bson.M{
		"_id": id,
	})
	if err != nil {
		return err
	}

	// delete chunks, even if file is missing
	res2, err := b.chunks.DeleteMany(ctx, bson.M{
		"files_id": id,
	})
	if err != nil {
		return err
	}

	// return error if no chunks or files have been deleted
	if res1.DeletedCount == 0 && res2.DeletedCount == 0 {
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
func (b *Bucket) DownloadToStreamByName(ctx context.Context, name string, w io.Writer, opts ...*options.NameOptions) (int64, error) {
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

// Drop will drop the files and chunks collection.
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

	// reset flag
	b.indexMutex.Lock()
	b.indexEnsured = false
	b.indexMutex.Unlock()

	return nil
}

// Find will perform a query on the underlying files collection.
func (b *Bucket) Find(ctx context.Context, filter interface{}, opts ...*options.GridFSFindOptions) (ICursor, error) {
	// merge options
	opt := options.MergeGridFSFindOptions(opts...)

	// options are asserted by find method

	// prepare find options
	find := options.Find()
	if opt.BatchSize != nil {
		find.SetBatchSize(*opt.BatchSize)
	}
	if opt.Limit != nil {
		find.SetLimit(int64(*opt.Limit))
	}
	if opt.MaxTime != nil {
		find.SetMaxTime(*opt.MaxTime)
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

	return stream, nil
}

// OpenDownloadStreamByName will open a download stream for the file with the
// specified name.
func (b *Bucket) OpenDownloadStreamByName(ctx context.Context, name string, opts ...*options.NameOptions) (*DownloadStream, error) {
	// merge options
	opt := options.MergeNameOptions(opts...)

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

	return stream, nil
}

// OpenUploadStream will open an upload stream for a new file with the provided
// name.
func (b *Bucket) OpenUploadStream(ctx context.Context, name string, opts ...*options.UploadOptions) (*UploadStream, error) {
	return b.OpenUploadStreamWithID(ctx, primitive.NewObjectID(), name, opts...)
}

// OpenUploadStreamWithID will open an upload stream for a new file with the
// provided id and name.
func (b *Bucket) OpenUploadStreamWithID(ctx context.Context, id interface{}, name string, opts ...*options.UploadOptions) (*UploadStream, error) {
	// merge options
	opt := options.MergeUploadOptions(opts...)

	// assert supported options
	assertOptions(opt, map[string]string{
		"ChunkSizeBytes": supported,
		"Metadata":       supported,
		"Registry":       ignored,
	})

	// ensure indexes
	err := b.ensureIndexes(ctx)
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
func (b *Bucket) UploadFromStream(ctx context.Context, name string, r io.Reader, opts ...*options.UploadOptions) (primitive.ObjectID, error) {
	// prepare id
	id := primitive.NewObjectID()

	// upload from stream
	err := b.UploadFromStreamWithID(ctx, id, name, r, opts...)
	if err != nil {
		return primitive.ObjectID{}, err
	}

	return id, nil
}

// UploadFromStreamWithID will upload a new file using the contents read from
// the provided reader.
func (b *Bucket) UploadFromStreamWithID(ctx context.Context, id interface{}, name string, r io.Reader, opts ...*options.UploadOptions) error {
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

func (b *Bucket) ensureIndexes(ctx context.Context) error {
	// acquire mutex
	b.indexMutex.Lock()
	defer b.indexMutex.Unlock()

	// return if indexes have been ensured
	if b.indexEnsured {
		return nil
	}

	// clone collection with primary read preference
	files, err := b.files.Clone(options.Collection().SetReadPreference(readpref.Primary()))
	if err != nil {
		return err
	}

	// count documents
	err = files.FindOne(ctx, bson.M{}).Err()
	if err != nil && err != ErrNoDocuments {
		return err
	}

	// set flag and skip if not empty
	if err == nil {
		b.indexEnsured = true
		return nil
	}

	// ensure context
	if ctx == nil {
		ctx = context.Background()
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
		buffer:    make([]byte, gridfs.UploadBufferSize),
	}
}

// Abort will abort the upload and remove uploaded chunks.
func (s *UploadStream) Abort() error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if stream has been closed
	if s.closed {
		return gridfs.ErrStreamClosed
	}

	// delete uploaded chunks
	_, err := s.bucket.chunks.DeleteMany(s.context, bson.M{
		"files_id": s.id,
	})
	if err != nil {
		return err
	}

	// set flag
	s.closed = true

	return nil
}

// Close will finish the upload and close the stream.
func (s *UploadStream) Close() error {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if stream has been closed
	if s.closed {
		return gridfs.ErrStreamClosed
	}

	// upload buffered data
	if s.bufLen > 0 {
		err := s.upload(true)
		if err != nil {
			return err
		}
	}

	// prepare file
	file := BucketFile{
		ID:         s.id,
		Length:     s.length,
		ChunkSize:  s.chunkSize,
		UploadDate: time.Now(),
		Filename:   s.name,
		Metadata:   s.metadata,
	}

	// write file
	_, err := s.bucket.files.InsertOne(s.context, file)
	if err != nil {
		return err
	}

	// set flag
	s.closed = true

	return nil
}

// Write will write the provided data to chunks in the background.
func (s *UploadStream) Write(data []uint8) (int, error) {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if stream has been closed
	if s.closed {
		return 0, gridfs.ErrStreamClosed
	}

	// upload data in chunks
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
			ID:   primitive.NewObjectID(),
			File: s.id,
			Num:  s.chunks + len(chunks),
			Data: s.buffer[i : i+size],
		})

		// update counter
		chunkedBytes += size
	}

	// write chunks
	_, err := s.bucket.chunks.InsertMany(s.context, chunks)
	if err != nil {
		return err
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

// Skip will advance the read head by the specified amount of bytes.
func (s *DownloadStream) Skip(skip int64) (int64, error) {
	return s.Seek(skip, io.SeekCurrent)
}

// Seek will reposition the read head using the specified values.
func (s *DownloadStream) Seek(offset int64, whence int) (int64, error) {
	// acquire mutex
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if closed
	if s.closed {
		return 0, gridfs.ErrStreamClosed
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
		position = s.file.Length - int(offset)
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
		return 0, gridfs.ErrStreamClosed
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
		return gridfs.ErrStreamClosed
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

	// set chunks
	s.chunks = s.file.Length / s.file.ChunkSize

	// seek to zero
	err = s.seek(0)
	if err != nil {
		return err
	}

	return nil
}

func (s *DownloadStream) seek(position int) error {
	// check position
	if position >= s.file.Length {
		s.cursor = nil
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
		return gridfs.ErrWrongIndex
	} else if num == s.chunks-1 && len(chunk.Data) != s.file.ChunkSize {
		return gridfs.ErrWrongSize
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
	// advance cursor
	if s.cursor == nil || !s.cursor.Next(s.context) {
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
		return gridfs.ErrWrongIndex
	} else if chunk.Num == s.chunks-1 && len(chunk.Data) != s.file.ChunkSize {
		return gridfs.ErrWrongSize
	}

	// set chunk
	s.chunk = &chunk

	// set buffer
	s.buffer = chunk.Data

	return nil
}
