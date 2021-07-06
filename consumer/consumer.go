package consumer

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/r-franke/cfrabbit/config"
	"github.com/streadway/amqp"
	"strings"
	"time"
)

type Consumer struct {
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
	consumer        *Consumer
	queueName       string
	Deliveries      <-chan amqp.Delivery
	routingkeys     []string
	exchangeName    string
}

const (
	reconnectDelay = 5 * time.Second
	reInitDelay    = 2 * time.Second
	maxRetries     = 5
)

var (
	reconnectRetries = 0
)

//goland:noinspection GoUnusedExportedFunction
func NewConsumer(queueName, exchangeName, exchangeType string, routingkeys []string) (Consumer, error) {
	consumer := Consumer{
		done:         make(chan bool),
		queueName:    queueName,
		routingkeys:  routingkeys,
		exchangeName: exchangeName,
	}
	go consumer.handleReconnect(config.RMQConnectionString)

	// Wait until consumer ready
	for {
		if consumer.isReady {
			break
		}
	}

	config.InfoLogger.Printf("Declaring exchange: %s, with type: %s\n", exchangeName, exchangeType)

	err := consumer.channel.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil)
	if err != nil {
		return Consumer{}, err
	}

	return consumer, nil
}

func (c *Consumer) bindQueues() error {

	config.InfoLogger.Printf("Declaring queue: %s\n", c.queueName)
	_, err := c.channel.QueueDeclare(
		c.queueName,
		true,                                   // Durable
		strings.Contains(c.queueName, "-dev-"), // Delete when unused
		false,                                  // Exclusive
		false,                                  // No-wait
		nil,                                    // Arguments
	)

	if err != nil {
		return err
	}

	for _, rk := range c.routingkeys {
		config.InfoLogger.Printf("Binding queue: %s to exchangeName: %s with routingkey: %s\n", c.queueName, c.exchangeName, rk)
		err = c.channel.QueueBind(c.queueName, rk, c.exchangeName, false, nil)
		if err != nil {
			return err
		}
	}

	return nil

}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (c *Consumer) handleReconnect(addr string) {
	for {
		c.isReady = false

		conn, err := c.connect(addr)

		if err != nil {
			if reconnectRetries > maxRetries {
				config.ErrorLogger.Fatalf("Max retries for reconnect reached, restarting...")
			}
			reconnectRetries++
			config.ErrorLogger.Println("Failed to connect:")
			config.ErrorLogger.Println(err)
			config.ErrorLogger.Printf("Retrying attempt %d out of %d...", reconnectRetries, maxRetries)

			select {
			case <-c.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		reconnectRetries = 0

		if done := c.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (c *Consumer) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.DialTLS(addr, &config.TlsConfig)

	if err != nil {
		return nil, err
	}

	c.changeConnection(conn)
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (c *Consumer) handleReInit(conn *amqp.Connection) bool {
	for {
		c.isReady = false

		err := c.init(conn)

		if err != nil {
			config.ErrorLogger.Println("Failed to initialize channel. Retrying...")

			select {
			case <-c.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		err = c.bindQueues()
		if err != nil {
			config.ErrorLogger.Println("Failed to bind queues. Retrying...")

			select {
			case <-c.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-c.done:
			return true
		case <-c.notifyConnClose:
			config.InfoLogger.Println("Connection closed. Reconnecting...")
			return false
		case <-c.notifyChanClose:
			config.InfoLogger.Println("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel
func (c *Consumer) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	c.changeChannel(ch)

	c.Deliveries, err = ch.Consume(
		c.queueName,
		fmt.Sprintf("%s-%s", config.AppName, uuid.NewString()), // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
	if err != nil {
		return err
	}

	c.isReady = true

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (c *Consumer) changeConnection(connection *amqp.Connection) {
	c.connection = connection
	c.notifyConnClose = make(chan *amqp.Error)
	c.connection.NotifyClose(c.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (c *Consumer) changeChannel(channel *amqp.Channel) {
	c.channel = channel
	c.notifyChanClose = make(chan *amqp.Error)
	c.notifyConfirm = make(chan amqp.Confirmation, 1)
	c.channel.NotifyClose(c.notifyChanClose)
	c.channel.NotifyPublish(c.notifyConfirm)
}
