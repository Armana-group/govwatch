// Lifted from koinos-indexer internal/amqprpc/client.go (same author) on 2026-10-24;
// only the error values were localised.
// Package amqprpc implements bounded, single-flight Koinos binary read RPC.
package amqprpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	amqp "github.com/rabbitmq/amqp091-go"

	"net"
	"strconv"
	"time"
)

type Client struct {
	url        string
	timeout    time.Duration
	gate       chan struct{}
	conn       *amqp.Connection
	channel    *amqp.Channel
	socket     net.Conn
	replies    <-chan amqp.Delivery
	returned   <-chan amqp.Return
	closed     <-chan *amqp.Error
	queue      string
	generation uint64
	stopped    bool
}

func New(url string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Client{url: url, timeout: timeout, gate: make(chan struct{}, 1)}
}
func (c *Client) Call(parent context.Context, service string, body []byte) (result []byte, generation uint64, err error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	if e := ctx.Err(); e != nil {
		return nil, 0, e
	}
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
	defer func() { <-c.gate }()
	if c.stopped {
		return nil, 0, ErrUnavailable
	}
	if c.conn == nil {
		if e := c.connect(ctx); e != nil {
			if ctx.Err() != nil {
				return nil, 0, ctx.Err()
			}
			return nil, 0, ErrUnavailable
		}
	}
	generation = c.generation
	raw := c.socket
	finish := closeOnCancel(ctx, raw)
	deadline, _ := ctx.Deadline()
	_ = raw.SetWriteDeadline(deadline)
	defer func() {
		if !finish() {
			result, err = nil, ctx.Err()
		}
		if err != nil {
			c.disconnect()
		} else {
			_ = raw.SetWriteDeadline(time.Time{})
		}
	}()
	var nonce [16]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return nil, generation, ErrUnavailable
	}
	id := hex.EncodeToString(nonce[:])
	expiration := strconv.FormatInt(max(1, time.Until(deadline).Milliseconds()), 10)
	if e := c.channel.PublishWithContext(ctx, "koinos.rpc", "koinos.rpc."+service, true, false, amqp.Publishing{ContentType: "application/octet-stream", CorrelationId: id, ReplyTo: c.queue, Expiration: expiration, Body: body}); e != nil {
		if ctx.Err() != nil {
			return nil, generation, ctx.Err()
		}
		return nil, generation, ErrUnavailable
	}
	for {
		select {
		case reply, ok := <-c.replies:
			if !ok {
				return nil, generation, ErrUnavailable
			}
			if reply.CorrelationId != id {
				continue
			}
			if reply.ContentType != "application/octet-stream" {
				return nil, generation, ErrSource
			}
			return reply.Body, generation, nil
		case <-c.returned:
			return nil, generation, ErrUnavailable
		case <-c.closed:
			return nil, generation, ErrUnavailable
		case <-ctx.Done():
			return nil, generation, ctx.Err()
		}
	}
}
func (c *Client) connect(ctx context.Context) (err error) {
	var raw net.Conn
	var stop func() bool
	defer func() {
		if stop != nil {
			if !stop() {
				err = ctx.Err()
			}
		}
		if err != nil {
			if raw != nil {
				raw.Close()
			}
			c.disconnect()
		}
	}()
	deadline, _ := ctx.Deadline()
	conn, err := amqp.DialConfig(c.url, amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, address string) (net.Conn, error) {
		var e error
		raw, e = (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		_ = raw.SetDeadline(deadline)
		socket := raw
		stop = closeOnCancel(ctx, socket)
		return raw, nil
	}})
	if err != nil {
		return err
	}
	c.conn = conn
	c.socket = raw
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	c.channel = ch
	if err = ch.ExchangeDeclarePassive("koinos.rpc", "direct", true, false, false, false, nil); err != nil {
		return err
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return err
	}
	if err = ch.QueueBind(q.Name, q.Name, "koinos.rpc", false, nil); err != nil {
		return err
	}
	c.queue = q.Name
	c.returned = ch.NotifyReturn(make(chan amqp.Return, 1))
	c.closed = ch.NotifyClose(make(chan *amqp.Error, 1))
	c.replies, err = ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	_ = raw.SetDeadline(time.Time{})
	c.generation++
	return nil
}

// The returned function must be called once before releasing the connection.
// A false result means cancellation started: join its socket close and retire
// this generation, including when a valid reply raced the cancellation.
func closeOnCancel(ctx context.Context, socket net.Conn) func() bool {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = socket.Close() })
	return func() bool {
		if stop() {
			return true
		}
		<-done
		return false
	}
}
func (c *Client) disconnect() {
	if c.socket != nil {
		c.socket.Close()
	}
	if c.conn != nil {
		_ = c.conn.CloseDeadline(time.Now().Add(time.Second))
	}
	c.conn = nil
	c.channel = nil
	c.socket = nil
	c.queue = ""
	c.replies = nil
	c.returned = nil
	c.closed = nil
}
func (c *Client) Close() error {
	c.gate <- struct{}{}
	defer func() { <-c.gate }()
	c.stopped = true
	c.disconnect()
	return nil
}
