package core

import (
	"time"

	"github.com/sandman-cs/core"
	"github.com/streadway/amqp"
)

var (
	rabbitConn       *amqp.Connection
	rabbitCloseError chan *amqp.Error
	amqpURI          string
	brokerUsr        string
	brokerPwd        string
	broker           string
	vHost            string
)

func init() {
	brokerUsr = "user"
	brokerPwd = "pwd"
	broker = "localhost"
	vHost = "/"

}

func initRabbitMQ(iBroker string, ivHost string, iUsr string, iPwd string) {

	//Override default values if they're provided...
	if iBroker != "" {
		broker = iBroker
	}
	if iUsr != "" {
		brokerUsr = iUsr
	}
	if iPwd != "" {
		brokerPwd = iPwd
	}
	if ivHost != "" {
		vHost = ivHost
	}

	amqpURI = "amqp://" + brokerUsr + ":" + brokerPwd + "@" + broker + vHost

	// create the rabbitmq error channel
	rabbitCloseError = make(chan *amqp.Error)

	// run the callback in a separate thread
	go rabbitConnector(amqpURI)

	// establish the rabbitmq connection by sending
	// an error and thus calling the error callback
	rabbitCloseError <- amqp.ErrClosed

	for rabbitConn == nil {
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
			core.SendMessage("Connecting to RabbitMQ")
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

		core.CheckError(err)
		core.SendMessage("Trying to reconnect to RabbitMQ")
		time.Sleep(500 * time.Millisecond)
	}
}
