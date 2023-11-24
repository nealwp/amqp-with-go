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


const (

    ConsumerAutoack = false
    ConsumerKey = "test.message.created"
    ConsumerName = "test.consumer.queue"

    DeadletterExchangeName = "test.exchange.deadletter"
    DeadletterQueue = "test.consumer.deadletter.queue"

    ExchangeAutodelete = false
    ExchangeDurable = true
    ExchangeInteral = false
    ExchangeName = "test.exchange"
    ExchangeNowait = false
    ExchangeType = "direct"

    PublisherKey = "test.message.consumed"
    PublisherMandatory = false
    PublisherImmediate = false
    QueueAutodelete = false
    QueueDurable = true 
    QueueExclusive = false
    QueueNolocal = false
    QueueNowait = false
    QueueType = "quorum"

)

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
        ExchangeName,
        ExchangeType,
        ExchangeDurable,
        ExchangeAutodelete,
        ExchangeInteral,
        ExchangeNowait,
        nil, // args
    )

    if err != nil {
        log.Panic("exchange declare failed")
    }

    err = consumerCh.ExchangeDeclare(
        DeadletterExchangeName,
        ExchangeType,
        ExchangeDurable,
        ExchangeAutodelete,
        ExchangeInteral,
        ExchangeNowait,
        nil,
    )

    if err != nil {
        log.Panic("deadletter exchange declare failed")
    }

    q, err := consumerCh.QueueDeclare(
        ConsumerName,
        QueueDurable,
        QueueAutodelete,
        QueueExclusive,
        QueueNowait,
        amqp.Table{
            "x-queue-type": QueueType,
            "x-deadletter-exchange": DeadletterExchangeName,
            "x-deadletter-routing-key": ConsumerKey,
        },
    )

    if err != nil {
        log.Panicf("error declaring queue: %s", err)
    }


    err = consumerCh.QueueBind(
        q.Name,
        ConsumerKey,
        ExchangeName,
        QueueNowait,
        nil,
    )

    if err != nil {
        log.Panic("failed to bind queue")
    }

    dlq, err := consumerCh.QueueDeclare(
        DeadletterQueue,
        QueueDurable,
        QueueAutodelete,
        QueueExclusive,
        QueueNowait,
        amqp.Table{
            "x-queue-type": QueueType,
        },
    )

    if err != nil {
        log.Panicf("error declaring deadletter queue: %s", err)
    }

    err = consumerCh.QueueBind(
        dlq.Name,
        ConsumerKey,
        DeadletterExchangeName,
        QueueNowait,
        nil,
    )

    if err != nil {
        log.Panicf("error binding deadletter queue: %s", err)
    }

    producerCh, err := conn.Channel()

    if err != nil {
        log.Panic("failed to open producer channel")
    }

    defer producerCh.Close() // the channel will close when main() returns

    err = producerCh.ExchangeDeclare(
        ExchangeName,
        ExchangeType,
        ExchangeDurable,
        ExchangeAutodelete,
        ExchangeInteral,
        ExchangeNowait,
        nil, // args
    )

    if err != nil {
        log.Panic("exchange declare failed")
    }

    msgs, err := consumerCh.Consume(
        q.Name,
        "", // consumer
        ConsumerAutoack,
        QueueExclusive,
        QueueNolocal,
        QueueNowait,
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

            err = producerCh.PublishWithContext(
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
        
}
