package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
)


func SetupRabbitMQConnection() (*amqp.Connection, error) {
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    return conn, err
}

func SetupChannels(c *amqp.Connection) (*amqp.Channel, *amqp.Channel, error) {
    consumer, err := c.Channel()
    if err != nil {
        return nil, nil, err
    }
    producer, err := c.Channel()
    return consumer, producer, err
}

func SetupExchanges(consumer *amqp.Channel, producer *amqp.Channel) (error) {
    err := consumer.ExchangeDeclare(
        ExchangeName,
        ExchangeType,
        ExchangeDurable,
        ExchangeAutodelete,
        ExchangeInteral,
        ExchangeNowait,
        nil, // args
    )

    if err != nil {
        return err
    }
    
    err = consumer.ExchangeDeclare(
        DeadletterExchangeName,
        ExchangeType,
        ExchangeDurable,
        ExchangeAutodelete,
        ExchangeInteral,
        ExchangeNowait,
        nil,
    )

    if err != nil {
        return err
    }

    err = producer.ExchangeDeclare(
        ExchangeName,
        ExchangeType,
        ExchangeDurable,
        ExchangeAutodelete,
        ExchangeInteral,
        ExchangeNowait,
        nil, // args
    )

    if err != nil {
        return err
    }

    return nil
}

func SetupQueues(ch *amqp.Channel) (*amqp.Queue, *amqp.Queue, error) {

    q, err := ch.QueueDeclare(
        ConsumerName,
        QueueDurable,
        QueueAutodelete,
        QueueExclusive,
        QueueNowait,
        amqp.Table{
            "x-queue-type": QueueType,
            "x-dead-letter-exchange": DeadletterExchangeName,
            "x-dead-letter-routing-key": ConsumerKey,
        },
    )

    if err != nil {
        return nil, nil, err
    }


    err = ch.QueueBind(
        q.Name,
        ConsumerKey,
        ExchangeName,
        QueueNowait,
        nil,
    )

    if err != nil {
        return nil, nil, err
    }

    dlq, err := ch.QueueDeclare(
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
        return nil, nil, err
    }

    err = ch.QueueBind(
        dlq.Name,
        ConsumerKey,
        DeadletterExchangeName,
        QueueNowait,
        nil,
    )

    if err != nil {
        return nil, nil, err
    }

    return &q, &dlq, nil
}
