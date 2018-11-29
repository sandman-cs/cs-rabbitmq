package coreRMQ

import (
	"time"

	"github.com/sandman-cs/core"
	"github.com/streadway/amqp"
)

var (
//rabbitConn       *amqp.Connection
//rabbitCloseError chan *amqp.Error
//amqpURI          string
)

func initRabbitMQ(broker string, vHost string, usr string, pwd string) {

	amqpURI := "amqp://" + usr + ":" + pwd + "@" + broker + vHost

	// create the rabbitmq error channel
	rabbitCloseError := make(chan *amqp.Error)

	// run the callback in a separate thread
	rabbitConnector(amqpURI)

	// establish the rabbitmq connection by sending
	// an error and thus calling the error callback
	rabbitCloseError <- amqp.ErrClosed
}

// re-establish the connection to RabbitMQ in case
// the connection has died
//
func rabbitConnector(uri string) {
	var rabbitErr *amqp.Error
	var rabbitConn *amqp.Connection

	go func() {
		for {
			rabbitErr = <-rabbitCloseError
			if rabbitErr != nil {
				core.SendMessage("Connecting to RabbitMQ")
				rabbitConn = connectToRabbitMQ(uri)
				rabbitCloseError = make(chan *amqp.Error)
				rabbitConn.NotifyClose(rabbitCloseError)
			}
		}
	}()

	for rabbitConn == nil {
		time.Sleep(1 * time.Second)
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

		core.CheckError(err)
		core.SendMessage("Trying to reconnect to RabbitMQ")
		time.Sleep(500 * time.Millisecond)
	}
}
