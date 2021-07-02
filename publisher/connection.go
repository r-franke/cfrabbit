package publisher

import (
	"github.com/r-franke/cfrabbit/config"
	"github.com/streadway/amqp"
	"time"
)

var reconnectRetries = 0

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (p *Publisher) handleReconnect(addr string) {
	for {
		p.isReady = false

		conn, err := p.connect(addr)

		if err != nil {
			if reconnectRetries > maxRetries {
				config.ErrorLogger.Fatalf("Max retries for reconnect reached, restarting...")
			}
			reconnectRetries++
			config.ErrorLogger.Println("Failed to connect:")
			config.ErrorLogger.Println(err)
			config.ErrorLogger.Printf("Retrying attempt %d out of %d...", reconnectRetries, maxRetries)

			select {
			case <-p.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		reconnectRetries = 0

		if done := p.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (p *Publisher) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.DialTLS(addr, &config.TlsConfig)

	if err != nil {
		return nil, err
	}

	p.changeConnection(conn)
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (p *Publisher) handleReInit(conn *amqp.Connection) bool {
	for {
		p.isReady = false

		err := p.init(conn)

		if err != nil {
			config.ErrorLogger.Println("Failed to initialize channel. Retrying...")

			select {
			case <-p.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-p.done:
			return true
		case <-p.notifyConnClose:
			config.InfoLogger.Println("Connection closed. Reconnecting...")
			return false
		case <-p.notifyChanClose:
			config.InfoLogger.Println("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel
func (p *Publisher) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	p.changeChannel(ch)

	p.isReady = true

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (p *Publisher) changeConnection(connection *amqp.Connection) {
	p.connection = connection
	p.notifyConnClose = make(chan *amqp.Error)
	p.connection.NotifyClose(p.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (p *Publisher) changeChannel(channel *amqp.Channel) {
	p.channel = channel
	p.notifyChanClose = make(chan *amqp.Error)
	p.notifyConfirm = make(chan amqp.Confirmation, 1)
	p.channel.NotifyClose(p.notifyChanClose)
	p.channel.NotifyPublish(p.notifyConfirm)
}
