package publisher

import (
	"errors"
	"github.com/r-franke/cfrabbit/config"
	"github.com/streadway/amqp"
	"time"
)

const (
	reconnectDelay = 5 * time.Second
	reInitDelay    = 2 * time.Second
	resendDelay    = 5 * time.Second
	maxRetries     = 5
)

type Publisher struct {
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
	ExchangeName    string
	ExchangeType    string
}

//goland:noinspection GoUnusedExportedFunction
func NewPublisher(exchangeName, exchangeType string) (Publisher, error) {
	publisher := Publisher{
		done:         make(chan bool),
		ExchangeName: exchangeName,
		ExchangeType: exchangeType,
	}
	go publisher.handleReconnect(config.RMQConnectionString)

	// Wait until publisher ready
	for {
		if publisher.isReady {
			break
		}
	}

	config.InfoLogger.Printf("Declaring exchange: %s, with type: %s\n", exchangeName, exchangeType)
	err := publisher.channel.ExchangeDeclare(exchangeName, exchangeType, true, false, false, false, nil)
	if err != nil {
		return Publisher{}, err
	}

	return publisher, nil
}

// Publish will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see unsafePublish.
func (p Publisher) Publish(routingkey string, data []byte) error {
	if !p.isReady {
		return errors.New("failed to push push: not connected")
	}
	for {
		err := p.unsafePublish(p.ExchangeName, routingkey, data)
		if err != nil {
			config.ErrorLogger.Println("Publish failed. Retrying...")
			select {
			case <-p.done:
				return errors.New("not connected to a server")
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-p.notifyConfirm:
			if confirm.Ack {
				return nil
			}
		case <-time.After(resendDelay):
		}
		config.InfoLogger.Println("Publish didn't confirm. Retrying...")
	}
}

// unsafePublish will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (p Publisher) unsafePublish(exchange string, routingkey string, data []byte) error {
	if !p.isReady {
		return errors.New("not connected to a server")
	}
	return p.channel.Publish(
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
