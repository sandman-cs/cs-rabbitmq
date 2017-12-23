package main

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)

var (
	rabbitConn       *amqp.Connection
	rabbitCloseError chan *amqp.Error
	amqpURI          string
	
)

func initRabbitMQ() {

	amqpURI = "amqp://" + conf.BrokerUser + ":" + conf.BrokerPwd + "@" + conf.Broker + conf.BrokerVhost

	// create the rabbitmq error channel
	rabbitCloseError = make(chan *amqp.Error)

	// run the callback in a separate thread
	go rabbitConnector(amqpURI)

	// establish the rabbitmq connection by sending
	// an error and thus calling the error callback
	SendMessage("Connecting to Broker---")
	rabbitCloseError <- amqp.ErrClosed

	for rabbitConn == nil {
		SendMessage("Waiting for Connection to Broker...")
		time.Sleep(1 * time.Second)
	}
}

// re-establish the connection to RabbitMQ in case
// the connection has died
//
func rabbitConnector(uri string) {
	var rabbitErr *amqp.Error

	for {
		rabbitErr = <-rabbitCloseError
		if rabbitErr != nil {
			sendMessage("Connecting to RabbitMQ")
			rabbitConn = connectToRabbitMQ(uri)
			rabbitCloseError = make(chan *amqp.Error)
			rabbitConn.NotifyClose(rabbitCloseError)
		}
	}
}

// Try to connect to the RabbitMQ server as
// long as it takes to establish a connection
//
func connectToRabbitMQ(uri string) *amqp.Connection {
	for {
		conn, err := amqp.Dial(uri)

		if err == nil {
			return conn
		}

		checkError(err)
		sendMessage("Trying to reconnect to RabbitMQ")
		time.Sleep(500 * time.Millisecond)
	}
}
