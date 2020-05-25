package lungo

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var gridfsReplacements = map[string]string{
	"*mongo.Cursor":          "lungo.ICursor",
	"*gridfs.DownloadStream": "*lungo.DownloadStream",
	"*gridfs.UploadStream":   "*lungo.UploadStream",
}

func TestBucketSymmetry(t *testing.T) {
	a := methods(reflect.TypeOf(&Bucket{}), nil)
	b := methods(reflect.TypeOf(&gridfs.Bucket{}), gridfsReplacements, "SetReadDeadline", "SetWriteDeadline")
	for i := range b {
		b[i] = strings.Replace(b[i], "(", "(context.Context, ", 1)
		b[i] = strings.Replace(b[i], ", )", ")", 1)
	}

	assert.Subset(t, a, b)
}

func TestUploadStreamSymmetry(t *testing.T) {
	a := methods(reflect.TypeOf(&UploadStream{}), nil)
	b := methods(reflect.TypeOf(&gridfs.UploadStream{}), nil, "SetWriteDeadline")
	assert.Subset(t, a, b)
}

func TestDownloadStreamSymmetry(t *testing.T) {
	a := methods(reflect.TypeOf(&DownloadStream{}), nil)
	b := methods(reflect.TypeOf(&gridfs.DownloadStream{}), nil, "SetReadDeadline")
	assert.Subset(t, a, b)
}

func TestBucketBasic(t *testing.T) {
	bucketTest(t, func(t *testing.T, b *Bucket) {
		id, err := b.UploadFromStream(nil, "foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		csr, err := b.Find(nil, bson.M{})
		assert.NoError(t, err)
		assert.Len(t, readAll(csr), 1)

		err = b.Rename(nil, id, "bar")
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName(nil, "foo", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		buf.Reset()
		n, err = b.DownloadToStreamByName(nil, "bar", &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		err = b.Delete(nil, id)
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName(nil, "bar", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		err = b.Delete(nil, id)
		assert.Equal(t, ErrFileNotFound, err)

		err = b.Rename(nil, id, "foo")
		assert.Equal(t, ErrFileNotFound, err)

		err = b.Drop(nil)
		assert.NoError(t, err)
	})

	gridfsTest(t, func(t *testing.T, b *gridfs.Bucket, _ *mongo.Collection) {
		id, err := b.UploadFromStream("foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		csr, err := b.Find(bson.M{})
		assert.NoError(t, err)
		assert.Len(t, readAll(csr), 1)

		err = b.Rename(id, "bar")
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName("foo", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		buf.Reset()
		n, err = b.DownloadToStreamByName("bar", &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())

		err = b.Delete(id)
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName("bar", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())

		err = b.Delete(id)
		assert.Equal(t, ErrFileNotFound, err)

		err = b.Rename(id, "foo")
		assert.Equal(t, ErrFileNotFound, err)

		err = b.Drop()
		assert.NoError(t, err)
	})
}

func TestBucketEmptyFile(t *testing.T) {
	data := make([]byte, 0)

	bucketTest(t, func(t *testing.T, b *Bucket) {
		id, err := b.UploadFromStream(nil, "foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		n, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})

	gridfsTest(t, func(t *testing.T, b *gridfs.Bucket, chunks *mongo.Collection) {
		id, err := b.UploadFromStream("foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		n, err := chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})
}

func TestBucketBigFile(t *testing.T) {
	data := make([]byte, gridfs.UploadBufferSize*1.5)

	bucketTest(t, func(t *testing.T, b *Bucket) {
		id, err := b.UploadFromStream(nil, "foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		n, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(97), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})

	gridfsTest(t, func(t *testing.T, b *gridfs.Bucket, chunks *mongo.Collection) {
		id, err := b.UploadFromStream("foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		n, err := chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(97), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})
}

func TestBucketManyWrites(t *testing.T) {
	data := make([]byte, gridfs.UploadBufferSize/100*1.5)

	bucketTest(t, func(t *testing.T, b *Bucket) {
		stream, err := b.OpenUploadStream(nil, "foo")
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		for i := 0; i < 100; i++ {
			n, err := stream.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		}

		err = stream.Close()
		assert.NoError(t, err)

		n, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(97), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStreamByName(nil, "foo", &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)*100), n)
	})

	gridfsTest(t, func(t *testing.T, b *gridfs.Bucket, chunks *mongo.Collection) {
		stream, err := b.OpenUploadStream("foo")
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		for i := 0; i < 100; i++ {
			n, err := stream.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		}

		err = stream.Close()
		assert.NoError(t, err)

		n, err := chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(97), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStreamByName("foo", &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)*100), n)
	})
}

func TestBucketAbortUpload(t *testing.T) {
	data := make([]byte, gridfs.UploadBufferSize/100*1.5)

	bucketTest(t, func(t *testing.T, b *Bucket) {
		stream, err := b.OpenUploadStream(nil, "foo")
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		for i := 0; i < 100; i++ {
			n, err := stream.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		}

		n, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(64), n)

		err = stream.Abort()
		assert.NoError(t, err)

		n, err = b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)
	})

	gridfsTest(t, func(t *testing.T, b *gridfs.Bucket, chunks *mongo.Collection) {
		stream, err := b.OpenUploadStream("foo")
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		for i := 0; i < 100; i++ {
			n, err := stream.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		}

		n, err := chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(64), n)

		err = stream.Abort()
		assert.NoError(t, err)

		n, err = chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), n)
	})
}

func TestBucketReUpload(t *testing.T) {
	data := []byte("Hello World!")

	bucketTest(t, func(t *testing.T, b *Bucket) {
		id := primitive.NewObjectID()

		err := b.UploadFromStreamWithID(nil, id, "foo", bytes.NewReader(data))
		assert.NoError(t, err)

		n, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())

		/* second */

		err = b.UploadFromStreamWithID(nil, id, "foo", bytes.NewReader(data))
		assert.Error(t, err)

		n, err = b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)

		buf.Reset()
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})

	gridfsTest(t, func(t *testing.T, b *gridfs.Bucket, chunks *mongo.Collection) {
		id := primitive.NewObjectID()

		err := b.UploadFromStreamWithID(id, "foo", bytes.NewReader(data))
		assert.NoError(t, err)

		n, err := chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())

		/* second */

		err = b.UploadFromStreamWithID(id, "foo", bytes.NewReader(data))
		assert.Error(t, err)

		n, err = chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), n)

		buf.Reset()
		n, err = b.DownloadToStream(id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})
}

func TestBucketSeekDownload(t *testing.T) {
	data := make([]byte, 0, 255)
	for i := byte(0); i < 255; i++ {
		data = append(data, i)
	}

	bucketTest(t, func(t *testing.T, b *Bucket) {
		id, err := b.UploadFromStream(nil, "foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		stream, err := b.OpenDownloadStream(nil, id)
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

		n1, err = stream.Seek(10, io.SeekEnd)
		assert.NoError(t, err)
		assert.Equal(t, int64(245), n1)

		buf = make([]byte, 3)
		n2, err = stream.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, 3, n2)
		assert.Equal(t, []byte{245, 246, 247}, buf)

		/* underflow */

		n1, err = stream.Seek(-10, io.SeekStart)
		assert.Error(t, err)
		assert.Equal(t, ErrInvalidPosition, err)
	})
}

func TestBucketTracking(t *testing.T) {
	bucketTest(t, func(t *testing.T, b *Bucket) {
		b.EnableTracking()

		id, err := b.UploadFromStream(nil, "foo", strings.NewReader("Hello World!"))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(nil, id, &buf)
		assert.Error(t, err)
		assert.Equal(t, int64(0), n)
		assert.Equal(t, "", buf.String())
		assert.Equal(t, ErrFileNotFound, err)

		err = b.ClaimUpload(nil, id)
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

		err = b.Cleanup(nil, 0)
		assert.NoError(t, err)

		buf.Reset()
		n, err = b.DownloadToStreamByName(nil, "bar", &buf)
		assert.Equal(t, ErrFileNotFound, err)
		assert.Zero(t, n)
		assert.Empty(t, buf.String())
	})
}

func TestBucketUploadResuming(t *testing.T) {
	bucketTest(t, func(t *testing.T, b *Bucket) {
		b.EnableTracking()

		id := primitive.NewObjectID()
		opt := options.GridFSUpload().SetChunkSizeBytes(5)

		stream, err := b.OpenUploadStreamWithID(nil, id, "foo", opt)
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		_, err = stream.Write([]byte("Hello"))
		assert.NoError(t, err)

		n, err := stream.Suspend()
		assert.NoError(t, err)
		assert.Equal(t, int64(5), n)

		stream, err = b.OpenUploadStreamWithID(nil, id, "foo", opt)
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		n, err = stream.Resume()
		assert.NoError(t, err)
		assert.Equal(t, int64(5), n)

		_, err = stream.Write([]byte(" World!"))
		assert.NoError(t, err)

		err = stream.Close()
		assert.NoError(t, err)

		err = b.ClaimUpload(nil, id)
		assert.NoError(t, err)

		var buf bytes.Buffer
		n, err = b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(12), n)
		assert.Equal(t, "Hello World!", buf.String())
	})
}

func TestBucketTransaction(t *testing.T) {
	collectionTest(t, func(t *testing.T, c ICollection) {
		sess, err := c.Database().Client().StartSession()
		assert.NoError(t, err)

		b := NewBucket(c.Database(), options.GridFSBucket().SetName(c.Name()))

		err = b.EnsureIndexes(nil, true)
		assert.NoError(t, err)

		res, err := sess.WithTransaction(context.Background(), func(ctx ISessionContext) (interface{}, error) {

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

		b := NewBucket(c.Database(), options.GridFSBucket().SetName(c.Name()))

		res, err := sess.WithTransaction(context.Background(), func(ctx ISessionContext) (interface{}, error) {
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
