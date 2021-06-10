package config

import (
	"crypto/tls"
	"github.com/cloudfoundry-community/go-cfenv"
	"log"
	"os"
)

var (
	RMQConnectionString string
	TlsConfig           = tls.Config{}
	DevMode             bool
	AppName             string
)

func init() {
	log.Println("Loading settings")
	_, runningInCF := os.LookupEnv("VCAP_SERVICES")

	if runningInCF {
		DevMode = false
		loadCFEnvironment()
	} else {
		DevMode = true
		loadDevEnvironment()
	}
}

func loadCFEnvironment() {
	log.Println("Loading RMQ CF environment variables.")

	// Parse vars from CF Cloud Foundry
	appEnv, err := cfenv.Current()
	if err != nil {
		log.Fatal("Cannot load system-variables from cloud foundry!")
	}

	rabbitVars, _ := appEnv.Services.WithLabel("p.rabbitmq")
	if len(rabbitVars) > 1 {
		log.Println("Multiple Rabbit bindings discovered. Loading first one by default.")
	}
	credentials := rabbitVars[0].Credentials
	RMQConnectionString = credentials["uri"].(string)

	if RMQConnectionString == "" {
		log.Fatal("RMQ settings in CF env are incomplete!")
	}

	AppName = appEnv.Name
}

func loadDevEnvironment() {
	var found bool
	log.Print("Loading dev mode environment variables.")

	// Overwrite SSH settings when in local dev mode, otherwise cert errors might occur.
	TlsConfig.ServerName, found = os.LookupEnv("DEV_SERVER_NAME")
	if !found {
		log.Fatal("DEV_SERVER_NAME env-variable not found!")
	}
	TlsConfig.InsecureSkipVerify = true

	RMQConnectionString, found = os.LookupEnv("RMQ_DEV_URL")
	if !found {
		log.Fatal("DEV_RMQ_URL env-variable not found!")
	}

	AppName = "dev-instance"
}
