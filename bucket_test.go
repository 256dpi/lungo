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
}

func TestBucketEmptyFile(t *testing.T) {
	bucketTest(t, func(t *testing.T, b *Bucket) {
		data := make([]byte, 0)

		id, err := b.UploadFromStream(nil, "foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		chunks, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), chunks)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})
}

func TestBucketBigFile(t *testing.T) {
	bucketTest(t, func(t *testing.T, b *Bucket) {
		data := make([]byte, gridfs.UploadBufferSize*1.5)

		id, err := b.UploadFromStream(nil, "foo", bytes.NewReader(data))
		assert.NoError(t, err)
		assert.NotEmpty(t, id)

		chunks, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(97), chunks)

		var buf bytes.Buffer
		n, err := b.DownloadToStream(nil, id, &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), n)
		assert.Equal(t, data, buf.Bytes())
	})
}

func TestBucketManyWrites(t *testing.T) {
	bucketTest(t, func(t *testing.T, b *Bucket) {
		stream, err := b.OpenUploadStream(nil, "foo")
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		data := make([]byte, gridfs.UploadBufferSize/100*1.5)

		for i := 0; i < 100; i++ {
			n, err := stream.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		}

		err = stream.Close()
		assert.NoError(t, err)

		chunks, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(97), chunks)

		var buf bytes.Buffer
		n, err := b.DownloadToStreamByName(nil, "foo", &buf)
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)*100), n)
	})
}

func TestBucketAbortUpload(t *testing.T) {
	bucketTest(t, func(t *testing.T, b *Bucket) {
		stream, err := b.OpenUploadStream(nil, "foo")
		assert.NoError(t, err)
		assert.NotNil(t, stream)

		data := make([]byte, gridfs.UploadBufferSize/100*1.5)

		for i := 0; i < 100; i++ {
			n, err := stream.Write(data)
			assert.NoError(t, err)
			assert.Equal(t, len(data), n)
		}

		chunks, err := b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(64), chunks)

		err = stream.Abort()
		assert.NoError(t, err)

		chunks, err = b.chunks.CountDocuments(nil, bson.M{})
		assert.NoError(t, err)
		assert.Equal(t, int64(0), chunks)
	})
}

func TestBucketSeekDownload(t *testing.T) {
	bucketTest(t, func(t *testing.T, b *Bucket) {
		data := make([]byte, 0, 255)
		for i := byte(0); i < 255; i++ {
			data = append(data, i)
		}

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
