package wirekit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestHandler(t *testing.T) {
	runHandler(HandlerFunc(func(conn *Conn, op Operation) error {
		// pretty.Println(op)

		switch op := op.(type) {
		case *Query:
			if op.Document != nil && op.Document[0].Key == "isMaster" {
				return conn.Write(&Reply{
					Request:      67000 + op.Request,
					ResponseTo:   op.Request,
					AwaitCapable: true,
					Cursor:       0,
					Start:        0,
					Documents: []bson.D{
						{
							{Key: "ismaster", Value: true},
							{Key: "maxBsonObjectSize", Value: 16777216},
							{Key: "maxMessageSizeBytes", Value: 48000000},
							{Key: "maxWriteBatchSize", Value: 100000},
							{Key: "localTime", Value: time.Now()},
							{Key: "logicalSessionTimeoutMinutes", Value: 30},
							{Key: "connectionId", Value: 5210},
							{Key: "minWireVersion", Value: 0},
							{Key: "maxWireVersion", Value: 8},
							{Key: "readOnly", Value: false},
							{Key: "ok", Value: 1.0},
						},
					},
				})
			}
		case *Message:
			if op.Sections[0].Single == true && op.Sections[0].Document[0].Key == "isMaster" {
				return conn.Write(&Message{
					Request:    67000 + op.Request,
					ResponseTo: op.Request,
					Sections: []Section{
						{
							Single: true,
							Document: bson.D{
								{Key: "ismaster", Value: true},
								{Key: "maxBsonObjectSize", Value: 16777216},
								{Key: "maxMessageSizeBytes", Value: 48000000},
								{Key: "maxWriteBatchSize", Value: 100000},
								{Key: "localTime", Value: time.Now()},
								{Key: "logicalSessionTimeoutMinutes", Value: 30},
								{Key: "connectionId", Value: 5210},
								{Key: "minWireVersion", Value: 0},
								{Key: "maxWireVersion", Value: 8},
								{Key: "readOnly", Value: false},
								{Key: "ok", Value: 1.0},
							},
						},
					},
				})
			}

			if op.Sections[0].Single == true && op.Sections[0].Document[0].Key == "ping" {
				return conn.Write(&Message{
					Request:    67000 + op.Request,
					ResponseTo: op.Request,
					Sections: []Section{
						{
							Single: true,
							Document: bson.D{
								{Key: "ok", Value: 1},
							},
						},
					},
				})
			}

			if op.Sections[0].Single == true && op.Sections[0].Document[0].Key == "endSessions" {
				return conn.Write(&Message{
					Request:    67000 + op.Request,
					ResponseTo: op.Request,
					Sections: []Section{
						{
							Single: true,
							Document: bson.D{
								{Key: "ok", Value: 1},
							},
						},
					},
				})
			}
		}

		panic("NOT HANDLED")

		return nil
	}), func(addr string) {
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(addr))
		assert.NoError(t, err)

		err = client.Ping(context.Background(), nil)
		assert.NoError(t, err)

		err = client.Disconnect(context.Background())
		assert.NoError(t, err)
	}, func(err error) {
		println(err.Error())
	})
}

func TestSniffHandler(t *testing.T) {
	sniffConn("0.0.0.0:1337", HandlerFunc(func(conn *Conn, op Operation) error {
		// pretty.Println(op)

		return nil
	}), func(addr string) {
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(addr))
		assert.NoError(t, err)

		err = client.Ping(context.Background(), nil)
		assert.NoError(t, err)

		err = client.Disconnect(context.Background())
		assert.NoError(t, err)
	}, func(err error) {
		println(err.Error())
	})
}
