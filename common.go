package cfrabbit

import (
	"github.com/r-franke/cfrabbit/config"
	"github.com/streadway/amqp"
)

func Setup(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.DialTLS(url, &config.TlsConfig)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}
