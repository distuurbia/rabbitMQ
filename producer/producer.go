package main

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

func main() {
	conn, err := amqp.Dial("amqp://olegRabbit:olegRabbit@localhost:5672/")
	if err != nil {
		logrus.Fatalf("unable to open connect to RabbitMQ server. Error: %s", err)
	}

	defer func() {
		err = conn.Close()
		if err != nil {
			logrus.Fatalf("failed to close connection to RabbitMQ server error: %s", err)
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		logrus.Fatalf("failed to open channel. Error: %s", err)
	}
	
	defer func() {
		err = ch.Close()
		if err != nil {
			logrus.Fatalf("failed to close a channel error: %s", err)
		}
	}()
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		logrus.Fatalf("failed to declare a queue. Error: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "wassap, RabbitMQ"
	msgCount := 0
	for i := 0; i < 4000; i++{
		err = ch.PublishWithContext(ctx,
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
		if err != nil {
			logrus.Fatalf("failed to publish a message. Error: %s", err)
		}
		msgCount++

	}
	logrus.Printf("Sent %v\n", msgCount)
}