package wirekit

import (
	"io"
	"net"
)

func runHandler(handler Handler, fn func(addr string), reporter func(error)) {
	// get free port
	port, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	// ensure close
	var closed bool
	defer port.Close()

	go func() {
		for {
			conn, err := port.Accept()
			if err != nil {
				if !closed {
					reporter(err)
				}
				return
			}

			go func() {
				err := Serve(NewConn(conn), handler)
				if err != nil {
					reporter(err)
				}
			}()
		}
	}()

	// yield address
	fn("mongodb://" + port.Addr().String())

	// set closed
	closed = true
}

func sniffConn(addr string, handler Handler, fn func(addr string), reporter func(error)) {
	// create listener
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	// ensure close
	var closed bool
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if !closed {
					reporter(err)
				}
				return
			}

			go func() {
				fwdConn, err := net.Dial("tcp", addr)
				if err != nil {
					panic(err)
				}

				inR, inW := io.Pipe()
				outR, outW := io.Pipe()

				go func() {
					s := NewStream(inR, nil)
					for {
						op, err := s.Read()
						if err != nil {
							reporter(err)
						}

						err = handler.Handle(nil, op)
						if err != nil {
							panic(err)
						}
					}
				}()

				go func() {
					s := NewStream(outR, nil)
					for {
						op, err := s.Read()
						if err != nil {
							reporter(err)
						}

						err = handler.Handle(nil, op)
						if err != nil {
							panic(err)
						}
					}
				}()

				in := io.MultiWriter(conn, inW)
				out := io.MultiWriter(fwdConn, outW)

				errs := make(chan error, 2)
				go func() {
					_, err := io.Copy(in, fwdConn)
					errs <- err
				}()
				go func() {
					_, err := io.Copy(out, conn)
					errs <- err
				}()

				err = <-errs
				if err != nil {
					panic(err)
				}
			}()
		}
	}()

	// get port
	_, port, _ := net.SplitHostPort(listener.Addr().String())

	// yield address
	fn("mongodb://0.0.0.0:" + port)

	// set closed
	closed = true
}
