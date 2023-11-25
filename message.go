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

    go func() {
        for d := range msgs {
            var msg Message 
            err := json.Unmarshal(d.Body, &msg)
             
            if err != nil {
                log.Fatalf("Error parsing JSON: %s", err)
            }

            var data TestMessage

            err = json.Unmarshal(msg.Data, &data)

            if err != nil {
                log.Fatalf("Error parsing JSON: %s", err)
            }

            if data == (TestMessage{}) {

                var anyData map[string]interface{}

                err = json.Unmarshal(msg.Data, &anyData)

                if err != nil {
                    log.Fatalf("Error parsing JSON: %s", err)
                }

                log.Printf("parsed no Message data from message: %s", anyData)
                                
                err := d.Reject(false)

                if err != nil {
                    log.Fatalf("error rejecting message: %s", err)
                }

                continue
            }

            log.Printf("parsed message: %s", data)

            err = producer.PublishWithContext(
                ctx,
                ExchangeName,
                PublisherKey,
                PublisherMandatory,
                PublisherImmediate,
                amqp.Publishing{
                    DeliveryMode: amqp.Persistent,
                    ContentType: "text/plain",
                    Body: (msg.Data),
                },
            )
            
            if err != nil {
                log.Fatalf("Error publishing message: %s", err)
            }
        }
    }()

    <- forever 
    return nil
}
