// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oleksandr/bonjour"
)

const LINKSMART = `
╦   ╦ ╔╗╔ ╦╔═  ╔═╗ ╔╦╗ ╔═╗ ╦═╗ ╔╦╗
║   ║ ║║║ ╠╩╗  ╚═╗ ║║║ ╠═╣ ╠╦╝  ║
╩═╝ ╩ ╝╚╝ ╩ ╩  ╚═╝ ╩ ╩ ╩ ╩ ╩╚═  ╩
`

var confPath = flag.String("conf", "conf/device-gateway.json", "Device gateway configuration file path")
var BuildNumber = "N/A"
var Version = "N/A"

func main() {
	fmt.Print(LINKSMART)
	logger.Printf("Starting Device Gateway")
	logger.Printf("Version: %s", Version)
	logger.Printf("Build Number: %s", BuildNumber)
	flag.Parse()

	if *confPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	config, err := loadConfig(*confPath)
	if err != nil {
		logger.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}
	config.revise()
	err = config.discoverEndpoints()
	if err != nil {
		logger.Printf("Failed to discover endpoints: %v\n", err)
		os.Exit(1)
	}

	// Agents' process manager
	agentManager := newAgentManager(config)

	// Configure MQTT if required
	discoveryCh := make(chan string)
	mqttConnector, err := newMQTTConnector(config, agentManager.DataRequestInbox())
	if err != nil {
		logger.Printf("Failed to create mqtt connector: %s", err)
		os.Exit(1)
	}
	if mqttConnector != nil {
		agentManager.setPublishingChannel(mqttConnector.dataInbox())
		//discoveryCh = mqttConnector.discoveryCh
		go mqttConnector.start()
	}

	// Start agents
	go agentManager.start()

	// Expose device's resources via REST (include statics and local catalog)
	restServer, err := newRESTfulAPI(config, agentManager.DataRequestInbox())
	if err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}

	go restServer.start()

	// Register in Service Catalog
	unregisterService, err := registerInServiceCatalog(config, discoveryCh)
	if err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}

	// Register this gateway as a service via DNS-SD
	var bonjourS *bonjour.Server
	if config.DnssdEnabled {
		bonjourS, err = bonjour.Register(config.Description,
			DNSSDServiceTypeDGW,
			"",
			config.Protocols.HTTP.BindPort,
			[]string{},
			nil)
		if err != nil {
			logger.Printf("Failed to register DNS-SD service: %s", err.Error())
		} else {
			logger.Println("Registered service via DNS-SD using type", DNSSDServiceTypeDGW)
		}
	}

	// Ctrl+C handling
	handler := make(chan os.Signal, 1)
	signal.Notify(handler,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	for sig := range handler {
		if sig == os.Interrupt {
			logger.Println("Caught interrupt signal.")
			break
		}
	}

	// Stop bonjour registration
	if bonjourS != nil {
		bonjourS.Shutdown()
		time.Sleep(1e9)
	}

	// Shutdown all
	agentManager.stop()
	if mqttConnector != nil {
		mqttConnector.stop()
	}

	// Unregister from Service Catalog
	logger.Println("Unregister services...")
	unregisterService()

	logger.Println("Stopped")
	os.Exit(0)
}
