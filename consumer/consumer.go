package consumer

import (
	"github.com/r-franke/cfrabbit/config"
	"github.com/streadway/amqp"
	"log"
	"time"
)

//func exampleHandler(deliveries <-chan amqp.Delivery) {
//	for d := range deliveries {
//
//		DO SOMETHING WITH DATA IN d.body
//
//		_ = d.Ack(false)
//	}
//}

var inRecovery = false

type MessageHandler func(<-chan amqp.Delivery)

type Consumer struct {
	uri          string
	queue        string
	exchange     string
	errorChannel chan *amqp.Error
	connection   *amqp.Connection
	channel      *amqp.Channel
	closed       bool
	handlers     []MessageHandler
	routingKey   string
	deliveries   <-chan amqp.Delivery
}

//goland:noinspection GoUnusedExportedFunction
func NewConsumer(exchange string, queue string, routingkey string) *Consumer {
	c := Consumer{
		uri:        config.RMQConnectionString,
		queue:      queue,
		exchange:   exchange,
		handlers:   make([]MessageHandler, 0),
		routingKey: routingkey,
	}

	return &c
}

func (c *Consumer) Start() {
	c.connect()
	go c.reconnector()
	c.consume()

}

func (c *Consumer) AddMessageHandler(handler MessageHandler) {
	if !inRecovery {
		c.handlers = append(c.handlers, handler)
	}

	go handler(c.deliveries)
}

func (c *Consumer) Close() {
	log.Println("Closing connection")
	c.closed = true
	_ = c.channel.Close()
	_ = c.connection.Close()
}

func (c *Consumer) consume() {
	log.Println("Registering handler...")

	c.deliveries = c.registerQueueConsumer()

	log.Printf("Consumer registered %s! Processing messages...", c.queue)
}

func (c *Consumer) reconnector() {
	for {
		err := <-c.errorChannel
		if !c.closed {
			log.Printf("Reconnecting after connection closed: %s", err)

			c.connect()
			c.recoverConsumers()
		}
		log.Printf("Not reconnecting, consumer was closed manually")
		return
	}
}

func (c *Consumer) connect() {
	for {
		log.Printf("Consumer is connecting to RabbitMQ on %s, queue: %s\n", c.uri, c.queue)

		var err error
		c.connection, err = amqp.DialTLS(c.uri, &config.TlsConfig)
		if err != nil {
			log.Println("Connection to RabbitMQ failed. Retrying in 1 sec... ", err)
			log.Println(err.Error())
			time.Sleep(time.Second)
			continue
		}

		c.errorChannel = make(chan *amqp.Error)
		c.connection.NotifyClose(c.errorChannel)

		c.openChannel()
		queue := c.declareQueue()
		c.bindQueue(queue, c.routingKey)

		return
	}
}

func (c *Consumer) declareQueue() amqp.Queue {
	queue, err := c.channel.QueueDeclare(
		c.queue,        // name
		true,           // durable
		config.DevMode, // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)

	if err != nil {
		log.Fatalf("Cannot declare queue %s\n%s", c.queue, err.Error())
	}

	return queue
}

func (c *Consumer) bindQueue(queue amqp.Queue, routingKey string) {
	err := c.channel.QueueBind(
		queue.Name,
		routingKey,
		c.exchange,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Cannot bind queue %s to exchange %s\n%s", queue.Name, c.exchange, err.Error())
	}
}

func (c *Consumer) openChannel() {
	var err error
	c.channel, err = c.connection.Channel()
	if err != nil {
		log.Fatalf("Opening channel failed\n%s", err.Error())
	}
}

func (c *Consumer) registerQueueConsumer() <-chan amqp.Delivery {

	d, err := c.channel.Consume(
		c.queue, // forwarder
		"",      // unique id
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		log.Fatalf("Consuming messages from failed\n%s", err.Error())
	}
	return d
}

func (c *Consumer) recoverConsumers() {
	inRecovery = true
	for i := range c.handlers {
		var handler = c.handlers[i]

		log.Printf("Recovering handler %d...\n", i)

		c.deliveries = c.registerQueueConsumer()
		c.AddMessageHandler(handler)
	}
	log.Println("Consumer recovered! Continuing message processing...")
	inRecovery = false
}
