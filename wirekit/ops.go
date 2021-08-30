package wirekit

import (
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

type Operation interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

// TODO: Support query flags.
// TODO: Support query return fields selector.

// Query is sent to query to the database for documents.
type Query struct {
	Request  int32
	SlaveOK  bool
	Handle   string
	Skip     int32
	Return   int32
	Document bson.D
}

func (q *Query) Encode() ([]byte, error) {
	// prepare slice
	var slice []byte

	// add header
	start, slice := wiremessage.AppendHeaderStart(slice, q.Request, 0, wiremessage.OpQuery)

	// add flags
	slice = wiremessage.AppendQueryFlags(slice, 0)

	// add handle
	slice = wiremessage.AppendQueryFullCollectionName(slice, q.Handle)

	// add number to skip and return
	slice = wiremessage.AppendQueryNumberToSkip(slice, q.Skip)
	slice = wiremessage.AppendQueryNumberToReturn(slice, q.Return)

	// add query
	slice, err := bson.MarshalAppend(slice, q.Document)
	if err != nil {
		return nil, err
	}

	// update length
	slice = bsoncore.UpdateLength(slice, start, int32(len(slice)))

	return slice, nil
}

func (q *Query) Decode(bytes []byte) error {
	// read header
	length, requestID, responseTo, opcode, slice, ok := wiremessage.ReadHeader(bytes)
	if !ok {
		return errors.New("header not ok")
	}

	// set header
	q.Request = requestID

	// check response to
	if responseTo != 0 {
		return errors.New("unexpected response to")
	}

	// check opcode
	if opcode != wiremessage.OpQuery {
		return errors.New("unexpected opcode")
	}

	// check length
	if int32(len(bytes)) != length {
		return errors.New("invalid length")
	}

	// read flags
	flags, slice, ok := wiremessage.ReadQueryFlags(slice)
	if !ok {
		return errors.New("unable to read flags")
	}

	// check slave ok
	if flags&wiremessage.SlaveOK == wiremessage.SlaveOK {
		q.SlaveOK = true
		flags &^= wiremessage.SlaveOK
	}

	// check flags
	if flags != 0 {
		return errors.New("unsupported flags: " + flags.String())
	}

	// read handle
	q.Handle, slice, ok = wiremessage.ReadQueryFullCollectionName(slice)
	if !ok {
		return errors.New("unable to read handle")
	}

	// read number to skip
	q.Skip, slice, ok = wiremessage.ReadQueryNumberToSkip(slice)
	if !ok {
		return errors.New("unable to read number to skip")
	}

	// read number to return
	q.Return, slice, ok = wiremessage.ReadQueryNumberToReturn(slice)
	if !ok {
		return errors.New("unable to read number to return")
	}

	// read raw document
	rawQuery, slice, ok := wiremessage.ReadQueryQuery(slice)
	if !ok {
		return errors.New("unable to read document")
	}

	// decode document
	err := bson.Unmarshal(rawQuery, &q.Document)
	if err != nil {
		return err
	}

	// check length
	if len(slice) != 0 {
		return errors.New("remaining bytes")
	}

	return nil
}

// TODO: Support reply flags.

type Reply struct {
	Request      int32
	ResponseTo   int32
	AwaitCapable bool
	Cursor       int64
	Start        int32
	Documents    []bson.D
}

func (r *Reply) Encode() ([]byte, error) {
	// prepare slice
	var slice []byte

	// add header
	start, slice := wiremessage.AppendHeaderStart(slice, r.Request, r.ResponseTo, wiremessage.OpReply)

	// prepare flags
	var flags wiremessage.ReplyFlag
	if r.AwaitCapable {
		flags |= wiremessage.AwaitCapable
	}

	// add flags
	slice = wiremessage.AppendReplyFlags(slice, flags)

	// add cursor
	slice = wiremessage.AppendReplyCursorID(slice, r.Cursor)

	// add start
	slice = wiremessage.AppendReplyStartingFrom(slice, r.Start)

	// add returned
	slice = wiremessage.AppendReplyNumberReturned(slice, int32(len(r.Documents)))

	// add documents
	for _, doc := range r.Documents {
		var err error
		slice, err = bson.MarshalAppend(slice, doc)
		if err != nil {
			return nil, err
		}
	}

	// update length
	slice = bsoncore.UpdateLength(slice, start, int32(len(slice)))

	return slice, nil
}

func (r *Reply) Decode(bytes []byte) error {
	// read header
	length, requestID, responseTo, opcode, slice, ok := wiremessage.ReadHeader(bytes)
	if !ok {
		return errors.New("header not ok")
	}

	// set header
	r.Request = requestID
	r.ResponseTo = responseTo

	// check opcode
	if opcode != wiremessage.OpReply {
		return errors.New("unexpected opcode")
	}

	// check length
	if int32(len(bytes)) != length {
		return errors.New("invalid length")
	}

	// read flags
	flags, slice, ok := wiremessage.ReadReplyFlags(slice)
	if !ok {
		return errors.New("unable to read flags")
	}

	// get await capable
	if flags&wiremessage.AwaitCapable == wiremessage.AwaitCapable {
		r.AwaitCapable = true
		flags &^= wiremessage.AwaitCapable
	}

	// check flags
	if flags != 0 {
		return errors.New("unsupported flags: " + flags.String())
	}

	// read cursor
	r.Cursor, slice, ok = wiremessage.ReadReplyCursorID(slice)
	if !ok {
		return errors.New("unable to read cursor")
	}

	// read start
	r.Start, slice, ok = wiremessage.ReadReplyStartingFrom(slice)
	if !ok {
		return errors.New("unable to read starting from")
	}

	// read number returned
	n, slice, ok := wiremessage.ReadReplyNumberReturned(slice)
	if !ok {
		return errors.New("unable to read numbered returned")
	}

	// read query
	for i := 0; i < int(n); i++ {
		// read raw document
		var rawDoc bsoncore.Document
		rawDoc, slice, ok = wiremessage.ReadReplyDocument(slice)
		if !ok {
			return errors.New("unable to read document")
		}

		// decode document
		var doc bson.D
		err := bson.Unmarshal(rawDoc, &doc)
		if err != nil {
			return err
		}

		// add document
		r.Documents = append(r.Documents, doc)
	}

	// check length
	if len(slice) != 0 {
		return errors.New("remaining bytes")
	}

	return nil
}

type GetMore struct {
	Request int32
	Handle  string
	Return  int32
	Cursor  int64
}

func (m *GetMore) Encode() ([]byte, error) {
	// prepare slice
	var slice []byte

	// add header
	start, slice := wiremessage.AppendHeaderStart(slice, m.Request, 0, wiremessage.OpGetMore)

	// add zero
	slice = wiremessage.AppendGetMoreZero(slice)

	// add handle
	slice = wiremessage.AppendQueryFullCollectionName(slice, m.Handle)

	// add number to return
	slice = wiremessage.AppendQueryNumberToReturn(slice, m.Return)

	// add cursor id
	slice = wiremessage.AppendGetMoreCursorID(slice, m.Cursor)

	// update length
	slice = bsoncore.UpdateLength(slice, start, int32(len(slice)))

	return slice, nil
}

func (m *GetMore) Decode(bytes []byte) error {
	// read header
	length, requestID, responseTo, opcode, slice, ok := wiremessage.ReadHeader(bytes)
	if !ok {
		return errors.New("header not ok")
	}

	// set header
	m.Request = requestID

	// check response to
	if responseTo != 0 {
		return errors.New("unexpected response to")
	}

	// check opcode
	if opcode != wiremessage.OpGetMore {
		return errors.New("unexpected opcode")
	}

	// check length
	if int32(len(bytes)) != length {
		return errors.New("invalid length")
	}

	// get zero
	zero, slice, ok := wiremessage.ReadKillCursorsZero(slice)
	if !ok || zero != 0 {
		return errors.New("unable to read zero")
	}

	// read handle
	m.Handle, slice, ok = wiremessage.ReadQueryFullCollectionName(slice)
	if !ok {
		return errors.New("unable to read handle")
	}

	// read number to return
	m.Return, slice, ok = wiremessage.ReadQueryNumberToReturn(slice)
	if !ok {
		return errors.New("unable to read number to return")
	}

	// read cursor id
	m.Cursor, slice, ok = wiremessage.ReadReplyCursorID(slice)

	// check length
	if len(slice) != 0 {
		return errors.New("remaining bytes")
	}

	return nil
}

type KillCursors struct {
	Request int32
	Cursors []int64
}

func (c *KillCursors) Encode() ([]byte, error) {
	// prepare slice
	var slice []byte

	// add header
	start, slice := wiremessage.AppendHeaderStart(slice, c.Request, 0, wiremessage.OpKillCursors)

	// add zero
	slice = wiremessage.AppendKillCursorsZero(slice)

	// add cursor ids
	slice = wiremessage.AppendKillCursorsNumberIDs(slice, int32(len(c.Cursors)))
	slice = wiremessage.AppendKillCursorsCursorIDs(slice, c.Cursors)

	// update length
	slice = bsoncore.UpdateLength(slice, start, int32(len(slice)))

	return slice, nil
}

func (c *KillCursors) Decode(bytes []byte) error {
	// read header
	length, requestID, responseTo, opcode, slice, ok := wiremessage.ReadHeader(bytes)
	if !ok {
		return errors.New("header not ok")
	}

	// set header
	c.Request = requestID

	// check response to
	if responseTo != 0 {
		return errors.New("unexpected response to")
	}

	// check opcode
	if opcode != wiremessage.OpKillCursors {
		return errors.New("unexpected opcode")
	}

	// check length
	if int32(len(bytes)) != length {
		return errors.New("invalid length")
	}

	// read zero
	zero, slice, ok := wiremessage.ReadKillCursorsZero(slice)
	if !ok || zero != 0 {
		return errors.New("unable to read zero")
	}

	// read number of cursor ids
	n, slice, ok := wiremessage.ReadKillCursorsNumberIDs(slice)
	if !ok {
		return errors.New("unable to read number of cursor ids")
	}

	// read cursor ids
	c.Cursors, slice, ok = wiremessage.ReadKillCursorsCursorIDs(slice, n)
	if !ok {
		return errors.New("unable to read cursor ids")
	}

	// check length
	if len(slice) != 0 {
		return errors.New("remaining bytes")
	}

	return nil
}

// TODO: Support message flags.

type Section struct {
	Single     bool
	Document   bson.D
	Identifier string
	Documents  []bson.D
}

type Message struct {
	Request    int32
	ResponseTo int32
	Sections   []Section
}

func (m *Message) Encode() ([]byte, error) {
	// prepare slice
	var slice []byte

	// add header
	start, slice := wiremessage.AppendHeaderStart(slice, m.Request, m.ResponseTo, wiremessage.OpMsg)

	// add flags
	slice = wiremessage.AppendMsgFlags(slice, 0)

	// add sections
	for _, section := range m.Sections {
		if section.Single {
			// add section type
			slice = wiremessage.AppendMsgSectionType(slice, wiremessage.SingleDocument)

			// add document
			var err error
			slice, err = bson.MarshalAppend(slice, section.Document)
			if err != nil {
				return nil, err
			}
		} else {
			// add section type
			slice = wiremessage.AppendMsgSectionType(slice, wiremessage.DocumentSequence)

			// add size
			var subStart int32
			subStart, slice = bsoncore.ReserveLength(slice)

			// add identifier
			slice = wiremessage.AppendQueryFullCollectionName(slice, section.Identifier)

			// add documents
			for _, doc := range section.Documents {
				var err error
				slice, err = bson.MarshalAppend(slice, doc)
				if err != nil {
					return nil, err
				}
			}

			// update length
			slice = bsoncore.UpdateLength(slice, subStart, int32(len(slice))-subStart)
		}
	}

	// update length
	slice = bsoncore.UpdateLength(slice, start, int32(len(slice)))

	return slice, nil
}

func (m *Message) Decode(bytes []byte) error {
	// read header
	length, requestID, responseTo, opcode, slice, ok := wiremessage.ReadHeader(bytes)
	if !ok {
		return errors.New("header not ok")
	}

	// set header
	m.Request = requestID
	m.ResponseTo = responseTo

	// check opcode
	if opcode != wiremessage.OpMsg {
		return errors.New("unexpected opcode")
	}

	// check length
	if int32(len(bytes)) != length {
		return errors.New("invalid length")
	}

	// get flags
	flags, slice, ok := wiremessage.ReadMsgFlags(slice)
	if !ok {
		return errors.New("unable to read flags")
	}

	// check flags
	if flags != 0 {
		return errors.New("unsupported flags")
	}

	// get sections
	for len(slice) > 0 {
		var sectionType wiremessage.SectionType
		sectionType, slice, ok = wiremessage.ReadMsgSectionType(slice)
		if !ok {
			return errors.New("unable to read sections")
		}

		// check section type
		if sectionType != wiremessage.SingleDocument && sectionType != wiremessage.DocumentSequence {
			return errors.New("unsupported section type")
		}

		// handle single documents
		if sectionType == wiremessage.SingleDocument {
			// read raw document
			var rawDoc bsoncore.Document
			rawDoc, slice, ok = wiremessage.ReadMsgSectionSingleDocument(slice)
			if !ok {
				return errors.New("unable to read single document")
			}

			// unmarshal document
			var doc bson.D
			err := bson.Unmarshal(rawDoc, &doc)
			if err != nil {
				return err
			}

			// append section
			m.Sections = append(m.Sections, Section{
				Single:   true,
				Document: doc,
			})
		}

		// handle document sequence
		if sectionType == wiremessage.DocumentSequence {
			// read raw documents
			var identifier string
			var rawDocs []bsoncore.Document
			identifier, rawDocs, slice, ok = wiremessage.ReadMsgSectionDocumentSequence(slice)
			if !ok {
				return errors.New("unable to read document sequence")
			}

			// unmarshal documents
			var docs []bson.D
			for _, rawDoc := range rawDocs {
				var doc bson.D
				err := bson.Unmarshal(rawDoc, &doc)
				if err != nil {
					return err
				}
				docs = append(docs, doc)
			}

			// add section
			m.Sections = append(m.Sections, Section{
				Identifier: identifier,
				Documents:  docs,
			})
		}
	}

	// check length
	if len(slice) != 0 {
		return errors.New("remaining bytes")
	}

	return nil
}
