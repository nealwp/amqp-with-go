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

func main() {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

    if err != nil {
        log.Panic("failed to connect to rabbitmq")
    }

    defer conn.Close() // the connection will close when main() returns

    consumerCh, err := conn.Channel()

    if err != nil {
        log.Panic("failed to open channel")
    }

    defer consumerCh.Close() // the channel will close when main() returns

    err = consumerCh.ExchangeDeclare(
        "test.exchange",     // name
        "direct",   // type
        true,       // durable
        false,      // autodelete
        false,      // internal
        false,      // no-wait
        nil,        // args
    )

    if err != nil {
        log.Panic("exchange declare failed")
    }

    q, err := consumerCh.QueueDeclare(
        "test.consumer.queue",
        false,
        false,
        true,
        false,
        nil,
    )

    if err != nil {
        log.Panic("failed to declare queue")
    }

    err = consumerCh.QueueBind(
        q.Name,
        "test.message.created",
        "test.exchange",
        false,
        nil,
    )

    if err != nil {
        log.Panic("failed to bind queue")
    }

    producerCh, err := conn.Channel()

    if err != nil {
        log.Panic("failed to open producer channel")
    }

    defer producerCh.Close() // the channel will close when main() returns

    err = producerCh.ExchangeDeclare(
        "test.exchange",     // name
        "direct",   // type
        true,       // durable
        false,      // autodelete
        false,      // internal
        false,      // no-wait
        nil,        // args
    )

    if err != nil {
        log.Panic("exchange declare failed")
    }

    msgs, err := consumerCh.Consume(
        q.Name,
        "",
        true,
        false,
        false,
        false,
        nil,
    )

    if err != nil {
        log.Panic("failed to register consumer")
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

            log.Printf("parsed message: %s", data)

            var wildData map[string]interface{}

            err = json.Unmarshal(msg.Data, &wildData)

            if err != nil {
                log.Fatalf("Error parsing JSON: %s", err)
            }

            log.Printf("parsed crazy message: %s", wildData)

            err = producerCh.PublishWithContext(
                ctx,
                "test.exchange",
                "test.message.consumed",
                false, // mandatory
                false, // immediate
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
        
}
