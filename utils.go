package cfrabbit

import (
	"errors"
	"github.com/r-franke/cfrabbit/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
	"time"
)

type Session struct {
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
}

const (
	reconnectDelay = 5 * time.Second
	reInitDelay    = 2 * time.Second
	maxRetries     = 5
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	reconnectRetries = 0
)

//goland:noinspection GoUnusedExportedFunction
func New() *Session {
	session := Session{
		done: make(chan bool),
	}
	go session.handleReconnect(config.RMQConnectionString)

	// Wait until session ready
	for {
		if session.isReady {
			break
		}
	}
	return &session
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (s *Session) handleReconnect(addr string) {
	for {
		s.isReady = false

		conn, err := s.connect(addr)

		if err != nil {
			if reconnectRetries > maxRetries {
				config.ErrorLogger.Fatalf("Max retries for reconnect reached, restarting...")
			}
			reconnectRetries++
			config.ErrorLogger.Println("Failed to connect:")
			config.ErrorLogger.Println(err)
			config.ErrorLogger.Printf("Retrying attempt %d out of %d...", reconnectRetries, maxRetries)

			select {
			case <-s.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		reconnectRetries = 0

		if done := s.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (s *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.DialTLS(addr, &config.TlsConfig)

	if err != nil {
		return nil, err
	}

	s.changeConnection(conn)
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (s *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		s.isReady = false

		err := s.init(conn)

		if err != nil {
			config.ErrorLogger.Println("Failed to initialize channel. Retrying...")

			select {
			case <-s.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-s.done:
			return true
		case <-s.notifyConnClose:
			config.InfoLogger.Println("Connection closed. Reconnecting...")
			return false
		case <-s.notifyChanClose:
			config.InfoLogger.Println("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel
func (s *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	err = ch.Confirm(false)

	if err != nil {
		return err
	}
	s.changeChannel(ch)
	s.isReady = true

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (s *Session) changeConnection(connection *amqp.Connection) {
	s.connection = connection
	s.notifyConnClose = make(chan *amqp.Error)
	s.connection.NotifyClose(s.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (s *Session) changeChannel(channel *amqp.Channel) {
	s.channel = channel
	s.notifyChanClose = make(chan *amqp.Error)
	s.notifyConfirm = make(chan amqp.Confirmation, 1)
	s.channel.NotifyClose(s.notifyChanClose)
	s.channel.NotifyPublish(s.notifyConfirm)
}

//goland:noinspection GoUnusedExportedFunction
func CreateAndBindQueue(exchangeName, exchangeType, queueName string, routingkeys []string) error {
	session := New()
	if !session.isReady {
		return errNotConnected
	}

	config.InfoLogger.Printf("Declaring exchange: %s, with type: %s\n", exchangeName, exchangeType)
	err := session.channel.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil)
	if err != nil {
		return err
	}

	config.InfoLogger.Printf("Declaring queue: %s\n", queueName)
	_, err = session.channel.QueueDeclare(
		queueName,
		true,                                 // Durable
		strings.Contains(queueName, "-dev-"), // Delete when unused
		false,                                // Exclusive
		false,                                // No-wait
		nil,                                  // Arguments
	)

	if err != nil {
		return err
	}

	for _, rk := range routingkeys {
		config.InfoLogger.Printf("Binding queue: %s to exchangeName: %s with routingkey: %s\n", queueName, exchangeName, rk)
		err = session.channel.QueueBind(queueName, rk, exchangeName, false, nil)
		if err != nil {
			return err
		}
	}
	_ = session.Close()
	return nil
}

//goland:noinspection GoUnusedExportedFunction
func UnbindQueue(queueName, key, exchange string) error {

	session := New()
	if !session.isReady {
		return errNotConnected
	}

	err := session.channel.QueueUnbind(queueName, key, exchange, amqp.Table{})
	_ = session.Close()
	return err
}

// Close will cleanly shutdown the channel and connection.
func (s *Session) Close() error {
	if !s.isReady {
		return errAlreadyClosed
	}
	s.done <- true
	err := s.channel.Close()
	if err != nil {
		return err
	}
	return nil
}
