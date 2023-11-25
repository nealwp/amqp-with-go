package main

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
    QueueConsumer = ""
    QueueDurable = true 
    QueueExclusive = false
    QueueNolocal = false
    QueueNowait = false
    QueueType = "quorum"

)
