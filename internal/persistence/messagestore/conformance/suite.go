// Package conformance provides the observable contract suite every MessageStore
// provider must pass.
package conformance

import (
	"context"
	"errors"
	"testing"

	"github.com/ayeshLK/websubhub/internal/persistence/messagestore"
)

type Harness interface {
	Producer() messagestore.Producer
	Administrator() messagestore.Administrator
	Destination() messagestore.Destination
	DLQDestination() messagestore.Destination
}

type Factory func(*testing.T) Harness

func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("send receive and acknowledgement", func(t *testing.T) {
		h := factory(t)
		ctx := context.Background()
		consumer := open(t, ctx, h, "consumer-ack")
		message := sample("message-1")
		expectedBody := append([]byte(nil), message.Body...)
		if err := h.Producer().Send(ctx, h.Destination(), message); err != nil {
			t.Fatal(err)
		}
		message.Body[0] = 'X'
		message.Metadata["safe"] = "mutated"
		batch, err := consumer.Receive(ctx, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) != 1 || batch[0].Message.ID != message.ID || string(batch[0].Message.Body) != string(expectedBody) || batch[0].Message.Metadata["safe"] != "value" || batch[0].Message.ContentType != message.ContentType {
			t.Fatalf("received = %#v", batch)
		}
		if err := consumer.Ack(ctx, batch[0].Receipt); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("contiguous acknowledgement", func(t *testing.T) {
		h := factory(t)
		ctx := context.Background()
		consumer := open(t, ctx, h, "consumer-order")
		_ = h.Producer().Send(ctx, h.Destination(), sample("message-1"))
		_ = h.Producer().Send(ctx, h.Destination(), sample("message-2"))
		batch, err := consumer.Receive(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if err := consumer.Ack(ctx, batch[1].Receipt); !errors.Is(err, messagestore.ErrOutOfOrder) {
			t.Fatalf("out-of-order ack = %v", err)
		}
		if err := consumer.Ack(ctx, batch[0].Receipt); err != nil {
			t.Fatal(err)
		}
		if err := consumer.Ack(ctx, batch[1].Receipt); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("nack redelivery", func(t *testing.T) {
		h := factory(t)
		ctx := context.Background()
		consumer := open(t, ctx, h, "consumer-nack")
		_ = h.Producer().Send(ctx, h.Destination(), sample("message-1"))
		batch, _ := consumer.Receive(ctx, 1)
		if err := consumer.Nack(ctx, batch[0].Receipt, messagestore.NackOptions{}); err != nil {
			t.Fatal(err)
		}
		redelivered, err := consumer.Receive(ctx, 1)
		if err != nil || len(redelivered) != 1 || redelivered[0].Message.ID != "message-1" {
			t.Fatalf("redelivery = %#v, %v", redelivered, err)
		}
	})
	t.Run("dead letter advances source", func(t *testing.T) {
		h := factory(t)
		ctx := context.Background()
		consumer := open(t, ctx, h, "consumer-dlq")
		message := sample("message-1")
		_ = h.Producer().Send(ctx, h.Destination(), message)
		batch, _ := consumer.Receive(ctx, 1)
		record := messagestore.DeadLetter{Destination: h.DLQDestination(), Message: message, TopicID: "topic-1", SubscriptionID: "sub-1", FailureClass: "malformed", Attempt: 1}
		if err := consumer.DeadLetter(ctx, batch[0].Receipt, record); err != nil {
			t.Fatal(err)
		}
		dlq, err := h.Administrator().OpenConsumer(ctx, messagestore.ConsumerSpec{ID: "dlq-reader", Destination: h.DLQDestination(), StartPosition: messagestore.StartEarliest})
		if err != nil {
			t.Fatal(err)
		}
		delivered, err := dlq.Receive(ctx, 1)
		if err != nil || len(delivered) != 1 || delivered[0].Message.ID != message.ID {
			t.Fatalf("DLQ = %#v, %v", delivered, err)
		}
	})
	t.Run("closure intent and reconnect", func(t *testing.T) {
		h := factory(t)
		ctx := context.Background()
		consumer := open(t, ctx, h, "consumer-close")
		_ = h.Producer().Send(ctx, h.Destination(), sample("message-1"))
		batch, _ := consumer.Receive(ctx, 1)
		_ = consumer.Ack(ctx, batch[0].Receipt)
		if err := consumer.Close(ctx, messagestore.CloseTemporary); err != nil {
			t.Fatal(err)
		}
		if _, err := consumer.Receive(ctx, 1); !errors.Is(err, messagestore.ErrClosed) {
			t.Fatalf("closed receive = %v", err)
		}
		if err := consumer.Reconnect(ctx); err != nil {
			t.Fatal(err)
		}
		if err := consumer.Close(ctx, messagestore.ClosePermanent); err != nil {
			t.Fatal(err)
		}
		reopened := open(t, ctx, h, "consumer-close")
		replayed, err := reopened.Receive(ctx, 1)
		if err != nil || len(replayed) != 1 {
			t.Fatalf("permanent close did not reset progress: %#v %v", replayed, err)
		}
	})
	t.Run("capabilities", func(t *testing.T) {
		h := factory(t)
		capabilities, err := h.Administrator().Capabilities(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := capabilities.Validate(); err != nil {
			t.Fatal(err)
		}
	})
}

func open(t *testing.T, ctx context.Context, h Harness, id string) messagestore.Consumer {
	t.Helper()
	consumer, err := h.Administrator().OpenConsumer(ctx, messagestore.ConsumerSpec{ID: messagestore.ConsumerID(id), Destination: h.Destination(), StartPosition: messagestore.StartEarliest})
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func sample(id string) messagestore.Message {
	return messagestore.Message{ID: id, Body: []byte(`{"exact":true}`), ContentType: "application/json; charset=utf-8", Metadata: map[string]string{"safe": "value"}}
}
