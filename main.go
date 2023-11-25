package main

import (
	"log"
)

func main() {
    conn, err := SetupRabbitMQConnection()

    if err != nil {
        log.Panic("failed to connect to rabbitmq")
    }

    defer conn.Close() // the connection will close when main() returns

    consumerCh, producerCh, err := SetupChannels(conn)

    if err != nil {
        log.Panicf("error setting up channels: %s", err)
    }

    defer consumerCh.Close() 
    defer producerCh.Close()

    err = SetupExchanges(consumerCh, producerCh)

    if err != nil {
        log.Panicf("error setting up exchanges: %s", err)
    }

    q, _, err := SetupQueues(consumerCh) // we don't have to do anything with DLQ after it's setup

    if err != nil {
        log.Panicf("error setting up queues: %s", err)
    }

    err = HandleMessages(consumerCh, producerCh, q)    

    if err != nil {
        log.Panicf("error consuming messages: %s", err)
    }
}
