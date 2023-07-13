package main

import (
	"context"
	"fmt"
	"time"

	"github.com/caarlos0/env"
	"github.com/distuurbia/RabbitMQ/config"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)


type dbStruct struct{
	key string
	message string
	id uuid.UUID
}

func main() {
	var cfg config.Config
	if err := env.Parse(&cfg); err != nil {
		logrus.Fatalf("Failed to parse config: %v", err)
	}
	pool, err := connectPostgres(&cfg)
	if err != nil {
		logrus.Fatalf("failed to connect postgres error: %v", err)
	}
	conn, err := amqp.Dial("amqp://olegRabbit:olegRabbit@localhost:5672/")
	if err != nil {
		logrus.Fatalf("unable to open connect to RabbitMQ server error: %s", err)
	}

	defer func() {
		err = conn.Close()
		if err != nil {
			logrus.Fatalf("failed to close connection to RabbitMQ server error: %s", err)
		}
	}()
	
	ch, err := conn.Channel()
	if err != nil {
		logrus.Fatalf("failed to open a channel. Error: %s", err)
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
	messages, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		logrus.Fatalf("failed to register a consumer. Error: %s", err)
	}
	var pgStat []*dbStruct
	start := time.Now()
	for message := range messages{
		pgStat = append(pgStat, &dbStruct{id: uuid.New(), key: q.Name, message: string(message.Body)})
		if len(pgStat) == 4000 || time.Since(start) > time.Second{
			break
		}
	}
	connPgx, err := pool.Acquire(context.Background())
	if err != nil {
		logrus.Fatalf("failed to acquire connection from pool: %v", err)
	}
	writeToPostgres(connPgx, pgStat)

	logrus.Print("Succesfully read ", len(pgStat), " messages")
}

func connectPostgres(cfg *config.Config) (*pgxpool.Pool, error) {
	conf, err := pgxpool.ParseConfig(cfg.PostgresPathKafka)
	if err != nil {
		return nil, fmt.Errorf("error in method pgxpool.ParseConfig: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), conf)
	if err != nil {
		return nil, fmt.Errorf("error in method pgxpool.NewWithConfig: %v", err)
	}
	return pool, nil
}

func writeToPostgres(conn *pgxpool.Conn, messages []*dbStruct) {
	batch := &pgx.Batch{}
	for _, msg := range messages{
		batch.Queue("INSERT INTO kafkamsg (id, key, message) VALUES ($1, $2, $3)", msg.id, msg.key, msg.message)
	}

	br := conn.SendBatch(context.Background(), batch)
	for i := 0; i < len(messages); i++{
		_, err := br.Exec()
		if err != nil {
			logrus.Fatalf("failed to exec batch: %v", err)
		}
	}
	defer br.Close()
	defer conn.Release()
}