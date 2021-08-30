package wirekit

import (
	"net"
)

type Conn struct {
	conn   net.Conn
	stream *Stream
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{
		conn:   conn,
		stream: NewStream(conn, conn),
	}
}

func (c *Conn) Read() (Operation, error) {
	return c.stream.Read()
}

func (c *Conn) Write(op Operation) error {
	return c.stream.Write(op)
}

func (c *Conn) Close() error {
	return c.conn.Close()
}
