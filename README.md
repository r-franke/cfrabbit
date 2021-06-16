# CF Rabbit

CF Rabbit is a module that wraps interacting with a RabbitMQ (*RMQ*) server through Cloud Foundry (*CF*). It takes care of reconnecting in the background.

It's an adaptation from the [AMQP Example](https://pkg.go.dev/github.com/streadway/amqp#example-package) for a CF environment

## Dev mode
CF Rabbit identifiers the environment it runs in. If no CF environment variables are available then it assumes it's running in local dev mode.

If  you want to run it in dev mode then make sure the following environment variables are available:

- *`DEV_SERVER_NAME`* (`localhost` if running a local RMQ)
- *`DEV_RMQ_URL`* (full RMQ connection string)

**TLS verification is disabled in dev mode**

## How to use

`NewConsumer(queueName string, routingkeys []string, exchange string) (<-chan amqp.Delivery, error)`
`NewPublisher(exchangeName, exchangeType string) (*Publisher, error)`

## Credits
Thanks go out to creators and maintainers of these modules:
- [go-cfenv](https://github.com/cloudfoundry-community/go-cfenv)
- [streadway's amqp](https://github.com/streadway/amqp)
- [google's uuid](https://github.com/google/uuid)