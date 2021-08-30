package wirekit

import (
	"bufio"
	"errors"
	"io"

	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

type Stream struct {
	reader *bufio.Reader
	writer io.Writer
}

func NewStream(r io.Reader, w io.Writer) *Stream {
	return &Stream{
		reader: bufio.NewReader(r),
		writer: w,
	}
}

func (s *Stream) Read() (Operation, error) {
	// read length
	rawLength, err := s.reader.Peek(4)
	if err != nil {
		return nil, err
	}

	// get length
	length, _, ok := bsoncore.ReadLength(rawLength)
	if !ok {
		return nil, errors.New("unable to read length")
	}

	// read bytes
	bytes := make([]byte, length)
	_, err = io.ReadFull(s.reader, bytes)
	if err != nil {
		return nil, err
	}

	// parse header
	length, _, _, opcode, _, ok := wiremessage.ReadHeader(bytes)
	if !ok {
		return nil, errors.New("unable to read header")
	}

	// prepare operation
	var op Operation
	switch opcode {
	case wiremessage.OpQuery:
		op = new(Query)
	case wiremessage.OpReply:
		op = new(Reply)
	case wiremessage.OpGetMore:
		op = new(GetMore)
	case wiremessage.OpKillCursors:
		op = new(KillCursors)
	case wiremessage.OpMsg:
		op = new(Message)
	default:
		return nil, errors.New("unsupported opcode: " + opcode.String())
	}

	// decode op
	err = op.Decode(bytes)
	if err != nil {
		return nil, err
	}

	return op, nil
}

func (s *Stream) Write(op Operation) error {
	// encode operation
	bytes, err := op.Encode()
	if err != nil {
		return err
	}

	// write bytes
	_, err = s.writer.Write(bytes)
	if err != nil {
		return err
	}

	return err
}
