// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// MQTTConnector provides MQTT protocol connectivity
type MQTTConnector struct {
	pubCh                  chan AgentResponse
	subCh                  chan<- DataRequest
	serviceCatalogEndpoint string
	deviceClients          map[string][]*mqttClient
}

type mqttClient struct {
	uri             string
	clientID        string
	paho            paho.Client
	publisher       publisher
	subscriber      subscriber
	offlineBufferCh chan AgentResponse
}

type publisher struct {
	topic    string
	qos      byte
	retained bool
}

type subscriber struct {
	topic string
	qos   byte
	// for referencing in message handler
	subCh      chan<- DataRequest
	deviceName string
}

var WaitTimeout time.Duration = 0 // overriden by environment variable

func newMQTTConnector(conf *Config, dataReqCh chan<- DataRequest) (*MQTTConnector, error) {

	deviceClients := make(map[string][]*mqttClient)
	for di := range conf.devices {
		for pi := range conf.devices[di].Protocols {
			if conf.devices[di].Protocols[pi].Type == MQTTProtocolType {
				mqtt := conf.devices[di].Protocols[pi].MQTT
				var client mqttClient
				client.uri = mqtt.Client.URI
				client.clientID = fmt.Sprintf("dgw-%s-%d-%d", conf.ID, di, pi)
				if mqtt.PubTopic != "" {
					client.publisher.topic = mqtt.PubTopic
					client.publisher.qos = mqtt.PubQoS
					client.publisher.retained = mqtt.PubRetained
				}
				if mqtt.SubTopic != "" {
					client.subscriber.topic = mqtt.SubTopic
					client.subscriber.qos = mqtt.SubQoS
					client.subscriber.subCh = dataReqCh
					client.subscriber.deviceName = conf.devices[di].Name
				}
				client.offlineBufferCh = make(chan AgentResponse, mqtt.Client.OfflineBuffer)

				err := client.configure(mqtt.Client)
				if err != nil {
					return nil, fmt.Errorf("error creating paho client options: %s", err)
				}

				deviceClients[conf.devices[di].Name] = append(deviceClients[conf.devices[di].Name], &client)
			}
		}
	}

	// no device needs MQTT
	if len(deviceClients) == 0 {
		return nil, nil
	}

	return &MQTTConnector{
		pubCh:                  make(chan AgentResponse, 100), // buffer for outgoing congestion
		subCh:                  dataReqCh,
		serviceCatalogEndpoint: conf.ServiceCatalog.Endpoint,
		deviceClients:          deviceClients,
	}, nil
}

func (c *MQTTConnector) dataInbox() chan<- AgentResponse {
	return c.pubCh
}

func (c *MQTTConnector) start() {
	logger.Println("MQTTConnector.start()")
	for i := range c.deviceClients {
		for j := range c.deviceClients[i] {
			go c.deviceClients[i][j].connect(0)
		}
	}
	go c.publisher()
}

// reads outgoing messages from the pubCh und publishes them to the broker
func (c *MQTTConnector) publisher() {
	for resp := range c.pubCh {
		logger.Debugln("MQTTConnector.publisher() message:", string(resp.Payload))
		clients := c.deviceClients[resp.ResourceId]
		for _, client := range clients {
			if !client.paho.IsConnected() {
				bufferCap := cap(client.offlineBufferCh)
				if bufferCap == 0 {
					logger.Printf("MQTTConnector.publisher() %s: discarded data while not connected.", client.uri)
					continue
				}
				select {
				case client.offlineBufferCh <- resp:
					logger.Printf("MQTTConnector.publisher() %s: buffered while not connected (%d/%d)", client.uri, len(client.offlineBufferCh), bufferCap)
				default:
					logger.Printf("MQTTConnector.publisher() %s: discarded data while not connected. Buffer is full (%d/%d)", client.uri, len(client.offlineBufferCh), bufferCap)
				}
				continue
			}
			if resp.IsError {
				logger.Printf("MQTTConnector.publisher() %s: data ERROR from agent manager: %s", client.uri, resp.Payload)
				continue
			}

			token := client.paho.Publish(client.publisher.topic, client.publisher.qos, client.publisher.retained, resp.Payload)
			if WaitTimeout > 0 {
				if done := token.WaitTimeout(WaitTimeout); done && token.Error() != nil {
					logger.Printf("MQTTConnector.publisher() error publishing: %s", token.Error())
					continue // Note: this payload will be lost
				} else if !done {
					logger.Printf("MQTTConnector.publisher() publish timeout. Message may be lost.")
					continue
				}
			}
			logger.Printf("MQTTConnector.publisher() %s: published to %s", client.uri, client.publisher.topic)
		}
	}
}

func (c *MQTTConnector) stop() {
	logger.Println("MQTTConnector.stop()")
	for i := range c.deviceClients {
		for _, client := range c.deviceClients[i] {
			if client.paho.IsConnected() {
				client.paho.Disconnect(500)
			}
		}
	}
}

func (client *mqttClient) configure(config *MQTTProtocolConfig) error {
	options := paho.NewClientOptions().
		AddBroker(config.URI).
		SetClientID(client.clientID).
		SetCleanSession(true).
		SetConnectionLostHandler(client.onConnectionLostHandler).
		SetOnConnectHandler(client.onConnectHandler).
		SetAutoReconnect(false) // we take care of re-connect ourselves -> paho's MaxReconnectInterval has no effect

	// Username/password authentication
	if config.Username != "" {
		options.SetUsername(config.Username)
		options.SetPassword(config.Password)
	}

	// SSL/TLS
	if strings.HasPrefix(config.URI, "ssl") {
		tlsConfig := &tls.Config{}
		// Custom CA to auth broker with a self-signed certificate
		if config.CaFile != "" {
			caFile, err := ioutil.ReadFile(config.CaFile)
			if err != nil {
				return fmt.Errorf("error reading CA file %s: %s", config.CaFile, err)
			} else {
				tlsConfig.RootCAs = x509.NewCertPool()
				ok := tlsConfig.RootCAs.AppendCertsFromPEM(caFile)
				if !ok {
					return fmt.Errorf("error parsing CA certificate %s", config.CaFile)
				}
			}
		}
		// Certificate-based client authentication
		if config.CertFile != "" && config.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
			if err != nil {
				return fmt.Errorf("error loading client TLS credentials: %s", err)
			} else {
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
		}

		options.SetTLSConfig(tlsConfig)
	}

	client.paho = paho.NewClient(options)
	return nil
}

func (client *mqttClient) connect(backOff time.Duration) {
	for {
		var backOffMessage string
		if backOff != 0 {
			backOffMessage = fmt.Sprintf(" backOff %v", backOff)
		}
		logger.Printf("MQTTConnector.connect() %s: connecting as %s%s", client.uri, client.clientID, backOffMessage)
		time.Sleep(backOff)
		if client.paho.IsConnected() {
			break
		}
		token := client.paho.Connect()
		token.Wait()
		if token.Error() == nil {
			break
		}
		logger.Printf("MQTTConnector.connect() %s: failed to connect: %v", client.uri, token.Error().Error())
		if backOff == 0 {
			backOff = 10 * time.Second
		} else if backOff <= MQTTMaxReconnectInterval {
			backOff *= 2
			if backOff > MQTTMaxReconnectInterval {
				backOff = MQTTMaxReconnectInterval
			}
		}
	}
}

func (client *mqttClient) onConnectHandler(_ paho.Client) {
	logger.Printf("MQTTConnector.onConnected() %s: connected.", client.uri)

	// subscribe topic is set
	if client.subscriber.topic != "" {
		logger.Printf("MQTTConnector.onConnected() %s: will subscribe to %s", client.uri, client.subscriber.topic)
		client.paho.Subscribe(client.subscriber.topic, client.subscriber.qos, client.subscriber.messageHandler)
	} else {
		logger.Printf("MQTTConnector.onConnected() %s: no subscriptions.", client.uri)
	}

	// publish buffered messages to the broker
	bufferCap := cap(client.offlineBufferCh)
	for len(client.offlineBufferCh) > 0 {
		resp := <-client.offlineBufferCh

		if resp.IsError {
			logger.Printf("MQTTConnector.onConnected() %s: data ERROR from agent manager: %s", client.uri, resp.Payload)
			continue
		}

		token := client.paho.Publish(client.publisher.topic, client.publisher.qos, client.publisher.retained, resp.Payload)
		if WaitTimeout > 0 {
			if published := token.WaitTimeout(WaitTimeout); token.Error() != nil {
				logger.Printf("MQTTConnector.onConnected() error publishing: %s", token.Error())
				continue // Note: this payload will be lost
			} else if !published {
				logger.Println("MQTTConnector.onConnected() publish timeout. Message may be lost.")
				continue
			}
		}
		logger.Printf("MQTTConnector.onConnected() published buffered message to %s (%d/%d)", client.publisher.topic, len(client.offlineBufferCh)+1, bufferCap)
	}
}

func (client *mqttClient) onConnectionLostHandler(_ paho.Client, reason error) {
	logger.Printf("MQTTConnector.onConnectionLost() %s: %s", client.uri, reason.Error())
	go client.connect(0)
}

// processes incoming messages from the broker and writes DataRequets to the subCh
func (s *subscriber) messageHandler(_ paho.Client, msg paho.Message) {
	logger.Printf("MQTTConnector.messageHandler() message received: topic: %v payload: %v\n", msg.Topic(), msg.Payload())

	// Send Data Request
	dr := DataRequest{
		ResourceId: s.deviceName,
		Type:       DataRequestTypeWrite,
		Arguments:  msg.Payload(),
		Reply:      nil, // there will be **no reply** on the request/command execution
	}
	logger.Printf("MQTTConnector.messageHandler() Submitting data request %#v", dr)
	s.subCh <- dr
	// no response - blocking on waiting for one
}
