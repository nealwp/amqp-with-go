package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Message struct {
    Type string `json:"type"`
    Data []byte `json:"data"`
}

type TestMessage struct {
    Hello string `json:"hello"`
}

func HandleMessages(consumer, producer *amqp.Channel, q *amqp.Queue) (error) {

    msgs, err := consumer.Consume(
        q.Name,
        QueueConsumer,
        ConsumerAutoack,
        QueueExclusive,
        QueueNolocal,
        QueueNowait,
        nil,
    )

    if err != nil {
        return err
    }

    forever := make(chan struct{})
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

    defer cancel()

    results := make(chan TestMessage)

    go func() {
        err := processMessages(msgs, results)
        if err != nil {
            log.Fatalf("error processing messages: %s", err)
        }
    }()

    go func() {
        err := publishResultsWithContext(ctx, producer, results)
        if err != nil {
            log.Fatalf("error publishing result: %s", err)
        }
    }()

    <- forever 
    return nil
}

func processMessages(msgs <- chan amqp.Delivery, out chan TestMessage) (error) {

    for d := range msgs {

        var msg Message 

        err := json.Unmarshal(d.Body, &msg)

        if err != nil {
            log.Fatalf("error parsing message: %s", err)
            err := d.Reject(false)

            if err != nil {
                log.Fatalf("error rejecting message: %s", err)
            }
        }

        var data TestMessage

        err = json.Unmarshal(msg.Data, &data)

        if err != nil || data == (TestMessage{}) {
            log.Print("parsed no Message data from message")

            err := d.Reject(false)

            if err != nil {
                log.Fatalf("error rejecting message: %s", err)
            }

            continue
        }

        log.Printf("parsed message: %s", data)
        out <- data

        err = d.Ack(false)
        if err != nil {
            log.Fatalf("error acking delivery: %s", err)
        }
    }
    close(out)
    return nil
}

func publishResultsWithContext(ctx context.Context, producer *amqp.Channel, results <- chan TestMessage) (error) {

    for data := range results {

        msgBytes, err := json.Marshal(data)

        if err != nil {
            log.Fatalf("error marshalling message: %s", err)
        }

        err = producer.PublishWithContext(
            ctx,
            ExchangeName,
            PublisherKey,
            PublisherMandatory,
            PublisherImmediate,
            amqp.Publishing{
                DeliveryMode: amqp.Persistent,
                ContentType: "text/plain",
                Body: msgBytes,
            },
        )

        if err != nil {
            log.Fatalf("Error publishing message: %s", err)
        }
    }

    return nil
}
