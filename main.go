package main

import (
	"encoding/json"
	"log"

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

    defer conn.Close()

    ch, err := conn.Channel()

    if err != nil {
        log.Panic("failed to open channel")
    }

    defer ch.Close()

    err = ch.ExchangeDeclare(
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

    q, err := ch.QueueDeclare(
        "test.queue",
        false,
        false,
        true,
        false,
        nil,
    )

    if err != nil {
        log.Panic("failed to declare queue")
    }

    err = ch.QueueBind(
        q.Name,
        "test.message.created",
        "test.exchange",
        false,
        nil,
    )

    if err != nil {
        log.Panic("failed to bind queue")
    }

    msgs, err := ch.Consume(
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

    var forever chan struct{}

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
        }
    }()

    <- forever // what is this syntax??
        
}
