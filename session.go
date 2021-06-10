package cfrabbit

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/r-franke/cfrabbit/config"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type Session struct {
	infoLogger      *log.Logger
	errorLogger     *log.Logger
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}

type Publisher struct {
	session      *Session
	ExchangeName string
	ExchangeType string
}

const (
	reconnectDelay = 5 * time.Second
	reInitDelay    = 2 * time.Second
	resendDelay    = 5 * time.Second
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("session is shutting down")
)

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
//goland:noinspection GoUnusedExportedFunction
func New() *Session {
	session := Session{
		infoLogger:  log.New(os.Stdout, "", log.LstdFlags),
		errorLogger: log.New(os.Stderr, "", log.LstdFlags),
		done:        make(chan bool),
	}
	go session.handleReconnect(config.RMQConnectionString)
	return &session
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect(addr string) {
	for {
		session.isReady = false
		session.infoLogger.Println("Attempting to connect")

		conn, err := session.connect(addr)

		if err != nil {
			session.errorLogger.Println("Failed to connect. Retrying...")

			select {
			case <-session.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (session *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.DialTLS(addr, &config.TlsConfig)

	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	session.infoLogger.Println("RQM connected!")
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (session *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		session.isReady = false

		err := session.init(conn)

		if err != nil {
			session.errorLogger.Println("Failed to initialize channel. Retrying...")

			select {
			case <-session.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-session.done:
			return true
		case <-session.notifyConnClose:
			session.infoLogger.Println("Connection closed. Reconnecting...")
			return false
		case <-session.notifyChanClose:
			session.infoLogger.Println("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel
func (session *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	err = ch.Confirm(false)

	if err != nil {
		return err
	}
	session.changeChannel(ch)
	session.isReady = true
	session.infoLogger.Println("RMQ is setup!")

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *Session) changeConnection(connection *amqp.Connection) {
	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *Session) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChanClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

// Publish will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePublish.
func (publisher Publisher) Publish(routingkey string, data []byte) error {
	if !publisher.session.isReady {
		return errors.New("failed to push push: not connected")
	}
	for {
		err := publisher.session.UnsafePublish(publisher.ExchangeName, routingkey, data)
		if err != nil {
			publisher.session.errorLogger.Println("Publish failed. Retrying...")
			select {
			case <-publisher.session.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-publisher.session.notifyConfirm:
			if confirm.Ack {
				publisher.session.infoLogger.Println("Publish confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		publisher.session.infoLogger.Println("Publish didn't confirm. Retrying...")
	}
}

// UnsafePublish will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (session *Session) UnsafePublish(exchange string, routingkey string, data []byte) error {
	if !session.isReady {
		return errNotConnected
	}
	return session.channel.Publish(
		exchange,   // Exchange
		routingkey, // Routing key
		false,      // Mandatory
		false,      // Immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
}

//goland:noinspection GoUnusedExportedFunction
func NewConsumer(queueName string, routingkey string, exchange string) (<-chan amqp.Delivery, error) {
	session := New()
	if !session.isReady {
		return nil, errNotConnected
	}

	_, err := session.channel.QueueDeclare(
		queueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)

	if err != nil {
		return nil, err
	}

	err = session.channel.QueueBind(queueName, routingkey, exchange, false, nil)
	if err != nil {
		return nil, err
	}

	return session.channel.Consume(
		queueName,
		fmt.Sprintf("%s-%s", config.AppName, uuid.NewString()), // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
}

func NewPublisher(exchangeName, exchangeType string) (*Publisher, error) {
	session := New()
	err := session.channel.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		session:      session,
		ExchangeName: exchangeName,
		ExchangeType: exchangeType,
	}, nil
}

// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	if !session.isReady {
		return errAlreadyClosed
	}
	return nil
}
