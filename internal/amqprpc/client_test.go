package amqprpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestCancelledCall(t *testing.T) {
	c := New("amqp://127.0.0.1:1", time.Second)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, e := c.Call(ctx, "chain", nil)
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
func TestHandshakeDeadline(t *testing.T) {
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, e := ln.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 1024)
		for {
			if _, e = conn.Read(buf); e != nil {
				return
			}
		}
	}()
	c := New("amqp://guest:secret@"+ln.Addr().String(), 50*time.Millisecond)
	defer c.Close()
	start := time.Now()
	_, _, e = c.Call(context.Background(), "chain", nil)
	if e == nil || time.Since(start) > time.Second {
		t.Fatal("unbounded handshake", e)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("socket leaked")
	}
}

type pausedClose struct {
	net.Conn
	started, release, closed chan struct{}
}

func (c *pausedClose) Close() error {
	close(c.started)
	<-c.release
	close(c.closed)
	return nil
}

func TestCancellationCleanupJoinsStartedCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &pausedClose{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	finish := closeOnCancel(ctx, c)
	cancel()
	<-c.started
	done := make(chan bool, 1)
	go func() { done <- finish() }()
	select {
	case <-done:
		close(c.release)
		t.Fatal("cleanup returned while callback could still close the connection")
	case <-time.After(20 * time.Millisecond):
	}
	close(c.release)
	select {
	case reusable := <-done:
		if reusable {
			t.Fatal("cancelled connection marked reusable")
		}
	case <-time.After(time.Second):
		t.Fatal("callback was not joined")
	}
	select {
	case <-c.closed:
	default:
		t.Fatal("callback unfinished")
	}
}

func TestStoppedCancellationCannotCloseReusedConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &pausedClose{started: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
	finish := closeOnCancel(ctx, c)
	if !finish() {
		t.Fatal("uncancelled connection retired")
	}
	cancel()
	select {
	case <-c.started:
		t.Fatal("stopped callback ran")
	default:
	}
}
