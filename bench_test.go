package zmq4

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// Benchmark the allocations made Socket.RecvBytes when receiving a 3-part
// message.
func Benchmark_RecvBytes(b *testing.B) {
	part1 := []byte("xy")
	part2 := bytes.Repeat([]byte("x"), 64)
	part3 := bytes.Repeat([]byte("y"), 128)

	sock := startBenchmarkSocket(b, part1, part2, part3)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// expect to get three parts, with the third returning
		// hasMore=false
		_, err := sock.RecvBytes(0)
		if err != nil {
			b.Fatal(err)
		}
		hasMore, err := sock.GetRcvmore()
		if err != nil {
			b.Fatal(err)
		}
		if !hasMore {
			b.Fatal("expected reading part1 to return hasMore=true")
		}
		_, err = sock.RecvBytes(0)
		if err != nil {
			b.Fatal(err)
		}
		hasMore, err = sock.GetRcvmore()
		if err != nil {
			b.Fatal(err)
		}
		if !hasMore {
			b.Fatal("expected reading part2 to return hasMore=true")
		}
		_, err = sock.RecvBytes(0)
		if err != nil {
			b.Fatal(err)
		}
		hasMore, err = sock.GetRcvmore()
		if err != nil {
			b.Fatal(err)
		}
		if hasMore {
			b.Fatal("expected reading part3 to return hasMore=false")
		}
	}
}

// Benchmark the allocations made Socket.RecvBytes2 when receiving a 3-part
// message.
func Benchmark_RecvBytes2(b *testing.B) {
	part1 := []byte("xy")
	part2 := bytes.Repeat([]byte("x"), 64)
	part3 := bytes.Repeat([]byte("y"), 128)

	sock := startBenchmarkSocket(b, part1, part2, part3)

	// re-use a set of byte slices with appropriate capacity
	msg := [][]byte{
		make([]byte, 0, len(part1)),
		make([]byte, 0, len(part2)),
		make([]byte, 0, len(part3)),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var hasMore bool

		// expect to get three parts, with the third returning
		// hasMore=false
		msg[0], hasMore, _ = sock.RecvBytes2(0, msg[0])
		if !hasMore {
			b.Fatal("expected reading part1 to return hasMore=true")
		}
		msg[1], hasMore, _ = sock.RecvBytes2(0, msg[1])
		if !hasMore {
			b.Fatal("expected reading part2 to return hasMore=true")
		}
		msg[2], hasMore, _ = sock.RecvBytes2(0, msg[2])
		if hasMore {
			b.Fatal("expected reading part3 to return hasMore=false")
		}

		// reset the lengths of the byte slices for the next read
		msg[0] = msg[0][:0]
		msg[1] = msg[1][:0]
		msg[2] = msg[2][:0]
	}
}

// startBenchmarkSocket runs a TCP server which acts like a ROUTER socket
// which continuously sends the given multi-part message to a client once it
// connects, and returns a DEALER socket connected to that server.
//
// This uses a low-level TCP server rather than using libzmq to avoid
// allocations which would effect the measurements of the benchmarks.
func startBenchmarkSocket(b *testing.B, msg ...[]byte) *Socket {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { ln.Close() })

	addr := ln.Addr().String()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			b.Log("error accepting tcp connection:", err)
			return
		}
		defer conn.Close()

		// send a 64 byte greeting with a NULL mechanism
		greeting := []byte{
			0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x7F, 0x03, 0x00, 'N', 'U', 'L', 'L',
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		}
		if _, err := conn.Write(greeting); err != nil {
			b.Log("error writing greeting:", err)
			return
		}
		// read back the client's greeting
		if _, err := io.ReadFull(conn, greeting); err != nil {
			b.Log("error reading greeting:", err)
			return
		}

		// send a server READY command
		serverReady := []byte{
			0x04, 28,
			0x05, 'R', 'E', 'A', 'D', 'Y',
			11, 'S', 'o', 'c', 'k', 'e', 't', '-', 'T', 'y', 'p', 'e',
			0x00, 0x00, 0x00, 0x06, 'R', 'O', 'U', 'T', 'E', 'R',
		}
		if _, err := conn.Write(serverReady); err != nil {
			b.Log("error writing ready:", err)
			return
		}

		// receive the client's READY command
		clientReady := make([]byte, 43)
		if _, err := io.ReadFull(conn, clientReady); err != nil {
			b.Log("error reading ready:", err)
			return
		}

		// encode the multi-part message to send over the wire
		var buf bytes.Buffer
		for i := 0; i < len(msg); i++ {
			flags := 0x01 // hasMore
			if i == len(msg)-1 {
				flags = 0x00 // !hasMore
			}
			buf.WriteByte(byte(flags))
			buf.WriteByte(byte(len(msg[i])))
			buf.Write(msg[i])
		}
		data := buf.Bytes()

		// send the multi-part message every 10ms
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := conn.Write(data); err != nil {
				b.Log("error writing msg:", err)
				return
			}
			select {
			case <-ticker.C:
			case <-b.Context().Done():
				return
			}
		}
	}()

	zmqCtx, err := NewContext()
	if err != nil {
		b.Fatal(err)
	}
	sock, err := zmqCtx.NewSocket(DEALER)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { sock.Close() })
	if err := sock.Connect("tcp://" + addr); err != nil {
		b.Fatal(err)
	}
	return sock
}
