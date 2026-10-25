// Package live follows the node's block broadcasts and keeps the store at
// head, backfilling gaps and rolling back provisional rows on reorgs.
package live

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Delivery is one broadcast message. Ack must be called after the message's
// effects are committed.
type Delivery struct {
	RoutingKey string
	Body       []byte
	Ack        func() error
}

// Feed yields block broadcasts until ctx ends or the connection drops, at
// which point the channel is closed.
type Feed interface {
	Deliveries(ctx context.Context) (<-chan Delivery, error)
}

const (
	exchange   = "koinos.event"
	bindingKey = "koinos.block.#"
	prefetch   = 16
	// maxQueued and messageTTLms bound our own queue on the node's broker so
	// a stalled or stopped watcher cannot grow it without limit (block
	// messages carry full receipts). Whatever is dropped or expired is
	// recovered by the gap backfill.
	maxQueued    = 2000
	messageTTLms = 10 * 60 * 1000
)

type amqpFeed struct {
	url   string
	queue string
}

// NewAMQPFeed binds a durable queue "govwatch.<network>" to the node's
// koinos.event topic exchange with key koinos.block.# and manual acks.
func NewAMQPFeed(url, network string) Feed {
	return &amqpFeed{url: url, queue: "govwatch." + network}
}

func (f *amqpFeed) Deliveries(ctx context.Context) (<-chan Delivery, error) {
	conn, err := amqp.Dial(f.url)
	if err != nil {
		return nil, fmt.Errorf("live: dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("live: channel: %w", err)
	}
	fail := func(step string, err error) (<-chan Delivery, error) {
		conn.Close()
		return nil, fmt.Errorf("live: %s: %w", step, err)
	}
	if err := ch.ExchangeDeclarePassive(exchange, "topic", true, false, false, false, nil); err != nil {
		return fail("exchange", err)
	}
	q, err := ch.QueueDeclare(f.queue, true, false, false, false, amqp.Table{"x-max-length": int32(maxQueued), "x-overflow": "drop-head", "x-message-ttl": int32(messageTTLms)})
	if err != nil {
		return fail("queue", err)
	}
	if err := ch.QueueBind(q.Name, bindingKey, exchange, false, nil); err != nil {
		return fail("bind", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fail("qos", err)
	}
	// Exclusive: a second watcher on the same broker must fail loudly rather
	// than silently splitting the feed with this one.
	msgs, err := ch.Consume(q.Name, "", false, true, false, false, nil)
	if err != nil {
		return fail("consume", err)
	}
	out := make(chan Delivery)
	go func() {
		defer close(out)
		defer conn.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-msgs:
				if !ok {
					return
				}
				d := Delivery{RoutingKey: m.RoutingKey, Body: m.Body, Ack: func() error { return m.Ack(false) }}
				select {
				case out <- d:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
