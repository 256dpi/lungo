package lungo

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var gridfsReplacements = map[string]string{
	"*mongo.Collection":           "lungo.ICollection",
	"*mongo.Cursor":               "lungo.ICursor",
	"*mongo.GridFSDownloadStream": "lungo.IGridFSDownloadStream",
	"*mongo.GridFSUploadStream":   "lungo.IGridFSUploadStream",
	"*mongo.GridFSFile":           "lungo.IGridFSFile",
}

func TestBucketSymmetry(t *testing.T) {
	a := methods(reflect.TypeOf(&Bucket{}), nil)
	b := methods(reflect.TypeOf(&mongo.GridFSBucket{}), gridfsReplacements, "FindContext", "RenameContext", "DeleteContext", "DropContext", "SetReadDeadline", "SetWriteDeadline")
	for i := range b {
		b[i] = strings.Replace(b[i], ", )", ")", 1)
	}

	assert.Subset(t, a, b)
}

func TestUploadStreamSymmetry(t *testing.T) {
	a := methods(reflect.TypeOf(&UploadStream{}), nil)
	b := methods(reflect.TypeOf(&mongo.GridFSUploadStream{}), gridfsReplacements, "SetWriteDeadline")
	assert.Subset(t, a, b)
}

func TestDownloadStreamSymmetry(t *testing.T) {
	a := methods(reflect.TypeOf(&DownloadStream{}), nil)
	b := methods(reflect.TypeOf(&mongo.GridFSDownloadStream{}), gridfsReplacements, "SetReadDeadline")
	assert.Subset(t, a, b)
}

func TestBucketBasic(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		id, err := b.UploadFromStream(ctx, "foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(ctx, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		csr, err := b.Find(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Len(t, readAll(csr), 1)

		err = b.Rename(ctx, id, "bar")
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName(ctx, "foo", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		buf.Reset()
		n, err = b.DownloadToStreamByName(ctx, "bar", &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		err = b.Delete(ctx, id)
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName(ctx, "bar", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		err = b.Delete(ctx, id)
		assert.Equal(t, ErrFileNotFound, err)

		err = b.Rename(ctx, id, "foo")
		assert.Equal(t, ErrFileNotFound, err)

		err = b.Drop(ctx)
		assert.NoError(t, err)
	})

	gridfsTest(t, func(t *testing.T, ctx context.Context, b *mongo.GridFSBucket) {
		id, err := b.UploadFromStream(ctx, "foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(ctx, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		csr, err := b.Find(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Len(t, readAll(csr), 1)

		err = b.Rename(ctx, id, "bar")
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName(ctx, "foo", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		buf.Reset()
		n, err = b.DownloadToStreamByName(ctx, "bar", &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		err = b.Delete(ctx, id)
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName(ctx, "bar", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		err = b.Delete(ctx, id)
		assert.Equal(t, ErrFileNotFound, err)

		err = b.Rename(ctx, id, "foo")
		assert.Equal(t, ErrFileNotFound, err)

		err = b.Drop(ctx)
		assert.NoError(t, err)
	})
}

func TestBucketOpenDownloadStreamMissing(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, b *Bucket) {
		stream, err := b.OpenDownloadStream(nil, primitive.NewObjectID())
		assert.Equal(t, ErrFileNotFound, err)
		assert.Nil(t, stream)

		stream, err = b.OpenDownloadStreamByName(nil, "missing")
		assert.Equal(t, ErrFileNotFound, err)
		assert.Nil(t, stream)
	})
}

func TestBucketDeleteOrphanChunks(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, b *Bucket) {
		id, err := b.UploadFromStream(nil, "foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)

		// remove only the files row, leaving chunks behind
		_, err = b.files.DeleteOne(nil, bson.M{"_id": id})
		assert.NoError(t, err)

		// Delete must surface ErrFileNotFound (the orphan chunks are still
		// cleaned up internally so subsequent finds return nothing)
		err = b.Delete(nil, id)
		assert.Equal(t, ErrFileNotFound, err)

		n, err := b.chunks.CountDocuments(nil, bson.M{"files_id": id})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)
	})
}

func TestBucketEmptyFile(t *testing.T) {
	data := make([]byte, 0)

	bucketTest(t, 0, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		id, err := b.UploadFromStream(ctx, "foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		n, err := b.GetChunksCollection().CountDocuments(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(ctx, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})
}

func TestBucketBigFile(t *testing.T) {
	data := make([]byte, uploadBufferSize*1.5)

	bucketTest(t, 0, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		id, err := b.UploadFromStream(ctx, "foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		n, err := b.GetChunksCollection().CountDocuments(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(97), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(ctx, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})
}

func TestBucketManyWrites(t *testing.T) {
	data := make([]byte, uploadBufferSize/100*1.5)

	bucketTest(t, 0, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		stream, err := b.OpenUploadStream(ctx, "foo")
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		for i := 0; i < 100; i++ {
			n, err := stream.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		}

		err = stream.Close()
		assert.NoError(t, err)
		n, err := b.GetChunksCollection().CountDocuments(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(97), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStreamByName(ctx, "foo", &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)*100), n)
	})
}

func TestBucketAbortUpload(t *testing.T) {
	data := make([]byte, uploadBufferSize/100*1.5)

	bucketTest(t, 0, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		stream, err := b.OpenUploadStream(ctx, "foo")
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		for i := 0; i < 100; i++ {
			n, err := stream.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		}

		n, err := b.GetChunksCollection().CountDocuments(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(64), n)

		err = stream.Abort()
		assert.NoError(t, err)

		n, err = b.GetChunksCollection().CountDocuments(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)
	})
}

func TestBucketReUpload(t *testing.T) {
	data := []byte("Hello World!")

	bucketTest(t, 0, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		id := bson.NewObjectID()

		err := b.UploadFromStreamWithID(ctx, id, "foo", bytes.NewReader(data))
		assert.NoError(t, err)

		n, err := b.GetChunksCollection().CountDocuments(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(ctx, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())

		/* second */

		err = b.UploadFromStreamWithID(ctx, id, "foo", bytes.NewReader(data))
		assert.Error(t, err)

		n, err = b.GetChunksCollection().CountDocuments(ctx, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)

		buf.Reset()
		n, err = b.DownloadToStream(ctx, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})
}

func TestBucketSeekDownload(t *testing.T) {
	data := make([]byte, 1024)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	abstractSeekTest(t, reader)
	abstractSeekTest(t, reader)

	bucketTest(t, 128, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		_, ok := b.(*Bucket)
		if !ok {
			return // this is only for lungo
		}
		id, err := b.UploadFromStream(ctx, "foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		stream, err := b.OpenDownloadStream(ctx, id)
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		/* skip (forward) */

		n1, err := stream.Skip(10)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), n1)

		buf := make([]byte, 3)
		n2, err := stream.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, 3, n2)
		assert.Equal(t, []byte{10, 11, 12}, buf)

		dlStream := stream.(*DownloadStream)

		abstractSeekTest(t, dlStream)
		abstractSeekTest(t, dlStream)
	})
}

func abstractSeekTest(t *testing.T, stream io.ReadSeeker) {
	n1, err := stream.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n1)

	/* forward */

	n1, err = stream.Seek(10, io.SeekCurrent)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), n1)

	buf := make([]byte, 3)
	n2, err := stream.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 3, n2)
	assert.Equal(t, []byte{10, 11, 12}, buf)

	/* back */

	n1, err = stream.Seek(-5, io.SeekCurrent)
	assert.NoError(t, err)
	assert.Equal(t, int64(8), n1)

	buf = make([]byte, 3)
	n2, err = stream.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 3, n2)
	assert.Equal(t, []byte{8, 9, 10}, buf)

	/* absolute */

	n1, err = stream.Seek(20, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), n1)

	buf = make([]byte, 3)
	n2, err = stream.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 3, n2)
	assert.Equal(t, []byte{20, 21, 22}, buf)

	/* reverse */

	n1, err = stream.Seek(-10, io.SeekEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(1014), n1)

	buf = make([]byte, 3)
	n2, err = stream.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 3, n2)
	assert.Equal(t, []byte{246, 247, 248}, buf)

	/* underflow */

	n1, err = stream.Seek(-10, io.SeekStart)
	assert.Error(t, err)
	assert.Zero(t, n1)
	assert.True(t, strings.Contains(err.Error(), "negative position"))

	/* end */

	n1, err = stream.Seek(-1, io.SeekEnd)
	assert.NoError(t, err)
	assert.Equal(t, int64(1023), n1)

	buf = make([]byte, 2)
	n2, err = stream.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 1, n2)
	assert.Equal(t, []byte{255, 0}, buf)

	/* overflow */

	n1, err = stream.Seek(1048, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(1048), n1)

	buf = make([]byte, 3)
	n2, err = stream.Read(buf)
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)
	assert.Zero(t, n2)

	/* user after EOF */

	n1, err = stream.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n1)

	buf = make([]byte, 3)
	n2, err = stream.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 3, n2)
	assert.Equal(t, []byte{0, 1, 2}, buf)
}

func TestBucketTracking(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		buc, ok := b.(*Bucket)
		if !ok {
			return // this is only for lungo
		}
		buc.EnableTracking()

		id, err := b.UploadFromStream(nil, "foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(nil, id, &buf)
		assert.Error(t, err)
		assert.Equal(t, int64(0), n)
		assert.Equal(t, "", buf.String())
		assert.Equal(t, ErrFileNotFound, err)

		err = buc.claimUpload(nil, id)
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		csr, err := b.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Len(t, readAll(csr), 1)

		err = b.Delete(nil, id)
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		err = buc.cleanup(nil, 0)
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		buf.Reset()
		n, err = b.DownloadToStreamByName(nil, "foo", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())
	})
}

func TestBucketCleanupStaleUpload(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, b *Bucket) {
		b.EnableTracking()

		id, err := b.UploadFromStream(nil, "foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		// upload completed but never claimed → marker is in "uploaded" state
		err = b.Cleanup(nil, 0)
		assert.NoError(t, err)

		// the unclaimed file must be gone
		var buf bytes.Buffer
		n, err := b.DownloadToStream(nil, id, &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
	})
}

func TestBucketUploadResuming(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, ctx context.Context, b IGridFSBucket) {
		buc, ok := b.(*Bucket)
		if !ok {
			return // this is only for lungo
		}
		buc.EnableTracking()

		id := bson.NewObjectID()
		opt := options.GridFSUpload().SetChunkSizeBytes(5)

		stream, err := b.OpenUploadStreamWithID(nil, id, "foo", opt)
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		_, err = stream.Write([]byte("Hello"))
		assert.NoError(t, err)

		upStream := stream.(*UploadStream)

		n, err := upStream.Suspend()
		assert.NoError(t, err)
		assert.Equal(t, int64(5), n)

		stream, err = b.OpenUploadStreamWithID(nil, id, "foo", opt)
		assert.NoError(t, err)
		assert.NotNil(t, stream)
		upStream = stream.(*UploadStream)
		n, err = upStream.Resume()
		assert.NoError(t, err)
		assert.Equal(t, int64(5), n)

		_, err = stream.Write([]byte(" World!"))
		assert.NoError(t, err)

		err = stream.Close()
		assert.NoError(t, err)

		err = buc.claimUpload(nil, id)
		assert.NoError(t, err)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())
	})
}

func TestBucketUploadResumingMultiChunk(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, b *Bucket) {
		b.EnableTracking()

		id := primitive.NewObjectID()
		opt := options.GridFSUpload().SetChunkSizeBytes(2)

		stream, err := b.OpenUploadStreamWithID(nil, id, "foo", opt)
		assert.NoError(t, err)

		// write 4 chunks worth of data
		_, err = stream.Write([]byte("ABCDEFGH"))
		assert.NoError(t, err)

		n, err := stream.Suspend()
		assert.NoError(t, err)
		assert.Equal(t, int64(8), n)

		stream, err = b.OpenUploadStreamWithID(nil, id, "foo", opt)
		assert.NoError(t, err)

		n, err = stream.Resume()
		assert.NoError(t, err)
		assert.Equal(t, int64(8), n)

		_, err = stream.Write([]byte("IJ"))
		assert.NoError(t, err)

		err = stream.Close()
		assert.NoError(t, err)

		err = b.ClaimUpload(nil, id)
		assert.NoError(t, err)

		var buf bytes.Buffer
		_, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, "ABCDEFGHIJ", buf.String())
	})
}

func TestBucketTrackedDeleteDuringUpload(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, b *Bucket) {
		b.EnableTracking()
		err := b.EnsureIndexes(nil, true)
		assert.NoError(t, err)

		id := primitive.NewObjectID()
		markerID := primitive.NewObjectID()

		// simulate an in-progress upload by inserting an "uploading" marker
		_, err = b.GetMarkersCollection(nil).InsertOne(nil, &BucketMarker{
			ID:        markerID,
			File:      id,
			State:     BucketMarkerStateUploading,
			Timestamp: time.Now(),
		})
		assert.NoError(t, err)

		// Delete must refuse to clobber the uploading marker
		err = b.Delete(nil, id)
		assert.Equal(t, ErrUploadInProgress, err)

		// the uploading marker must be intact (same _id, still uploading)
		var existing BucketMarker
		err = b.GetMarkersCollection(nil).FindOne(nil, bson.M{"files_id": id}).Decode(&existing)
		assert.NoError(t, err)
		assert.Equal(t, markerID, existing.ID)
		assert.Equal(t, BucketMarkerStateUploading, existing.State)
	})
}

func TestBucketTrackedDeletePreservesMarkerID(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, b *Bucket) {
		b.EnableTracking()
		err := b.EnsureIndexes(nil, true)
		assert.NoError(t, err)

		// upload a file normally; the resulting "uploaded" marker has its
		// own _id which Delete must preserve when transitioning to deleted
		id, err := b.UploadFromStream(nil, "foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		var before BucketMarker
		err = b.GetMarkersCollection(nil).FindOne(nil, bson.M{"files_id": id}).Decode(&before)
		assert.NoError(t, err)
		assert.Equal(t, BucketMarkerStateUploaded, before.State)

		err = b.Delete(nil, id)
		assert.NoError(t, err)

		var after BucketMarker
		err = b.GetMarkersCollection(nil).FindOne(nil, bson.M{"files_id": id}).Decode(&after)
		assert.NoError(t, err)
		assert.Equal(t, before.ID, after.ID)
		assert.Equal(t, BucketMarkerStateDeleted, after.State)
	})
}

func TestBucketTrackedDeleteWithoutPriorMarker(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, b *Bucket) {
		b.EnableTracking()
		err := b.EnsureIndexes(nil, true)
		assert.NoError(t, err)

		// Delete on a never-uploaded id inserts a fresh deleted marker
		id := primitive.NewObjectID()
		err = b.Delete(nil, id)
		assert.NoError(t, err)

		var first BucketMarker
		err = b.GetMarkersCollection(nil).FindOne(nil, bson.M{"files_id": id}).Decode(&first)
		assert.NoError(t, err)
		assert.Equal(t, BucketMarkerStateDeleted, first.State)

		// a second Delete on the same id preserves the marker _id
		err = b.Delete(nil, id)
		assert.NoError(t, err)

		var second BucketMarker
		err = b.GetMarkersCollection(nil).FindOne(nil, bson.M{"files_id": id}).Decode(&second)
		assert.NoError(t, err)
		assert.Equal(t, first.ID, second.ID)
	})
}

func TestBucketTrackedEmptyUpload(t *testing.T) {
	bucketTest(t, 0, func(t *testing.T, b *Bucket) {
		b.EnableTracking()

		id := primitive.NewObjectID()
		stream, err := b.OpenUploadStreamWithID(nil, id, "foo")
		assert.NoError(t, err)

		// no Write — Close immediately
		err = stream.Close()
		assert.NoError(t, err)

		// claim and read back: should be empty but present
		err = b.ClaimUpload(nil, id)
		assert.NoError(t, err)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())
	})
}

func TestBucketTransaction(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		sess, err := c.Database().Client().StartSession()
		assert.NoError(t, err)

		options.GridFSBucket().SetName(c.Name())

		b := c.Database().GridFSBucket()

		buc, ok := b.(*Bucket)
		if !ok {
			return // this is only for lungo
		}
		err = buc.ensureIndexes(nil, true)
		assert.NoError(t, err)

		res, err := sess.WithTransaction(nil, func(ctx context.Context) (interface{}, error) {
			id, err := b.UploadFromStream(ctx, "foo", strings.NewReader("Hello World!"))
			if err != nil {
				return nil, err
			}

			var buf bytes.Buffer
			_, err = b.DownloadToStream(ctx, id, &buf)
			if err != nil {
				return nil, err
			}

			return buf.String(), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "Hello World!", res)
	})
}

func TestBucketTransactionError(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		sess, err := c.Database().Client().StartSession()
		assert.NoError(t, err)

		b := c.Database().GridFSBucket(options.GridFSBucket().SetName(c.Name()))

		res, err := sess.WithTransaction(nil, func(ctx context.Context) (interface{}, error) {
			id, err := b.UploadFromStream(ctx, "foo", strings.NewReader("Hello World!"))
			if err != nil {
				return nil, err
			}

			var buf bytes.Buffer
			_, err = b.DownloadToStream(ctx, id, &buf)
			if err != nil {
				return nil, err
			}

			return buf.String(), nil
		})
		assert.Error(t, err)
		assert.Nil(t, res)
	})
}
