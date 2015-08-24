package wendy

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestTCPTransport(t *testing.T) {
	baton := make(chan struct{}, 1)

	go func() {
		transport := NewTCPTransport()
		l, err := transport.Listen("0.0.0.0:2999")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()

		baton <- struct{}{}

		conn, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		<-baton

		var buf [1024]byte
		n, err := conn.Read(buf[:])
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(buf[:n], []byte("Hello Word")) {
			t.Fatalf("expected %q instead of %q", "Hello World", buf[:n])
		}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}

		baton <- struct{}{}
	}()

	func() {
		transport := NewTCPTransport()
		<-baton

		conn, err := transport.DialTimeout("127.0.0.1:2999", 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		_, err = io.WriteString(conn, "Hello World")
		if err != nil {
			t.Fatal(err)
		}

		baton <- struct{}{}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	<-baton
}
