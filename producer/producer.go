package producer

import (
	"github.com/r-franke/cfrabbit"
	"github.com/r-franke/cfrabbit/config"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type Publisher struct {
	url          string
	exchange     Exchange
	errorChannel chan *amqp.Error
	connection   *amqp.Connection
	channel      *amqp.Channel
	closed       bool
}

type Exchange struct {
	Name         string
	ExchangeType string
}

//goland:noinspection GoUnusedExportedFunction
func NewPublisher(exchange Exchange) *Publisher {
	p := Publisher{
		url:      config.RMQConnectionString,
		exchange: exchange,
	}

	p.connect()
	go p.reconnector()

	return &p
}

func (p *Publisher) connect() {
	for {
		log.Printf("Publisher is connecting to RabbitMQ on %s\n", p.url)

		var err error
		p.connection, p.channel, err = cfrabbit.Setup(p.url)
		if err != nil {
			log.Println("Connection to RabbitMQ failed. Retrying in 1 sec... ", err)
			log.Println(err.Error())
			time.Sleep(time.Second)
			continue
		}

		p.errorChannel = make(chan *amqp.Error)
		p.connection.NotifyClose(p.errorChannel)

		err = p.channel.ExchangeDeclare(p.exchange.Name, p.exchange.ExchangeType, true, false, false, false, nil)
		log.Fatalf("Cannot declare exchange %s\n%s", p.exchange.Name, err)

		return

	}
}
func (p *Publisher) Publish(routingKey string, body []byte) {

	err := p.channel.Publish(
		p.exchange.Name, // exchange
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})

	if err != nil {
		log.Printf("Error publishing message\n%s", err)
		p.errorChannel <- &amqp.Error{Reason: "Publishing message failed"}
	}
}

func (p *Publisher) reconnector() {
	for {
		err := <-p.errorChannel
		if !p.closed {
			log.Printf("Reconnecting after connection closed\n%s", err)

			p.connect()
		}
		log.Printf("Not reconnecting, consumer was closed manually")
		return
	}
}

func (p *Publisher) openChannel() {
	var err error
	p.channel, err = p.connection.Channel()
	if err != nil {
		log.Fatalf("Opening channel failed\n%s", err)
	}

}
