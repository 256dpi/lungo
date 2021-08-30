package wirekit

type Handler interface {
	Handle(conn *Conn, op Operation) error
}

type HandlerFunc func(conn *Conn, op Operation) error

func (f HandlerFunc) Handle(conn *Conn, op Operation) error {
	return f(conn, op)
}

func Serve(conn *Conn, handler Handler) error {
	// ensure close
	defer conn.Close()

	// handle operations
	for {
		// read next operation
		op, err := conn.Read()
		if err != nil {
			return err
		}

		// handle operation
		err = handler.Handle(conn, op)
		if err != nil {
			return err
		}
	}
}
