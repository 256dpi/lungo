package wirekit

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestQuery(t *testing.T) {
	query1 := Query{
		Request: 1,
		Handle:  "foo.bar",
		Skip:    2,
		Return:  3,
		Document: bson.D{
			{Key: "foo", Value: "bar"},
		},
	}

	bytes, err := query1.Encode()
	assert.NoError(t, err)

	var query2 Query
	err = query2.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, query1, query2)
}

func TestReply(t *testing.T) {
	reply1 := Reply{
		Request:    1,
		ResponseTo: 1,
		Cursor:     2,
		Start:      3,
		Documents: []bson.D{
			{
				{Key: "foo", Value: "bar"},
			},
			{
				{Key: "bar", Value: "baz"},
			},
		},
	}

	bytes, err := reply1.Encode()
	assert.NoError(t, err)

	var reply2 Reply
	err = reply2.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, reply1, reply2)
}

func TestGetMore(t *testing.T) {
	getMore1 := GetMore{
		Request: 1,
		Handle:  "foo.bar",
		Return:  3,
		Cursor:  4,
	}

	bytes, err := getMore1.Encode()
	assert.NoError(t, err)

	var getMore2 GetMore
	err = getMore2.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, getMore1, getMore2)
}

func TestKillCursors(t *testing.T) {
	killCursors1 := KillCursors{
		Request: 1,
		Cursors: []int64{2, 3},
	}

	bytes, err := killCursors1.Encode()
	assert.NoError(t, err)

	var killCursors2 KillCursors
	err = killCursors2.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, killCursors1, killCursors2)
}

func TestMessage(t *testing.T) {
	msg1 := Message{
		Request:    1,
		ResponseTo: 2,
		Sections: []Section{
			{
				Single: true,
				Document: bson.D{
					{Key: "foo", Value: "bar"},
				},
			},
			{
				Identifier: "foo",
				Documents: []bson.D{
					{
						{Key: "bar", Value: "baz"},
					},
					{
						{Key: "baz", Value: "quz"},
					},
				},
			},
		},
	}

	bytes, err := msg1.Encode()
	assert.NoError(t, err)

	var msg2 Message
	err = msg2.Decode(bytes)
	assert.NoError(t, err)
	assert.Equal(t, msg1, msg2)
}

func TestLocalMongo(t *testing.T) {
	rawConn, err := net.Dial("tcp", "0.0.0.0:1337")
	assert.NoError(t, err)

	conn := NewConn(rawConn)

	err = conn.Write(&Query{
		Request: 1,
		SlaveOK: true,
		Handle:  "admin.$cmd",
		Skip:    0,
		Return:  -1,
		Document: bson.D{
			{Key: "isMaster", Value: 1},
		},
	})
	assert.NoError(t, err)

	_, err = conn.Read()
	assert.NoError(t, err)

	err = conn.Write(&Message{
		Request: 2,
		Sections: []Section{
			{
				Single: true,
				Document: bson.D{
					{Key: "isMaster", Value: 1},
					{Key: "$db", Value: "admin"},
				},
			},
		},
	})
	assert.NoError(t, err)

	_, err = conn.Read()
	assert.NoError(t, err)

	err = conn.Close()
	assert.NoError(t, err)
}
