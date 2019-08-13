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

// MQTTConnector provides paho protocol connectivity
type MQTTConnector struct {
	pubCh                  chan AgentResponse
	subCh                  chan<- DataRequest
	serviceCatalogEndpoint string
	deviceClients          map[string][]*mqttClient
	//discoveryCh            chan string
}

type mqttClient struct {
	config          *MqttProtocolConfig
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
	for di, d := range conf.devices {
		for pi := range d.Protocols {
			if d.Protocols[pi].Type == MQTTProtocolType {
				mqtt := d.Protocols[pi].MQTT
				var client mqttClient
				client.config = mqtt.Client
				if mqtt.PubTopic != "" {
					client.publisher.topic = mqtt.PubTopic
					client.publisher.qos = mqtt.PubQoS
					client.publisher.retained = mqtt.PubRetained
				}
				if mqtt.SubTopic != "" {
					client.subscriber.topic = mqtt.SubTopic
					client.subscriber.qos = mqtt.SubQoS
					client.subscriber.subCh = dataReqCh
					client.subscriber.deviceName = d.Name
				}
				client.offlineBufferCh = make(chan AgentResponse, mqtt.Client.OfflineBuffer)

				options, err := client.pahoOptions(fmt.Sprintf("%s-%d-%d", conf.Id, di, pi))
				if err != nil {
					return nil, fmt.Errorf("error creating paho client options: %s", err)
				}
				client.paho = paho.NewClient(options)

				deviceClients[d.Name] = append(deviceClients[d.Name], &client)
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

// TODO return from constructor
func (c *MQTTConnector) dataInbox() chan<- AgentResponse {
	return c.pubCh
}

func (c *MQTTConnector) start() {
	logger.Println("MQTTConnector.start()")

	//if c.config.Discover {
	//	c.discoverBrokerEndpoint()
	//}

	for i := range c.deviceClients {
		for j := range c.deviceClients[i] {
			go c.deviceClients[i][j].connect(0)
		}
	}

	// configure the mqtt client
	//c.configureMqttConnection()

	// start the connection routine

	//go c.connect(0)

	// start the publisher routine
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
					logger.Println("MQTTConnector.publisher() got data while not connected to the broker. **discarded**")
					continue
				}
				select {
				case client.offlineBufferCh <- resp:
					logger.Printf("MQTTConnector.publisher() got data while not connected to the broker. Keeping in buffer (%d/%d)", len(client.offlineBufferCh), bufferCap)
				default:
					logger.Printf("MQTTConnector.publisher() got data while not connected to the broker. Buffer is full (%d/%d). **discarded**", len(client.offlineBufferCh), bufferCap)
				}
				continue
			}
			if resp.IsError {
				logger.Println("MQTTConnector.publisher() data ERROR from agent manager:", string(resp.Payload))
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
			logger.Println("MQTTConnector.publisher() published to", client.publisher.topic)
		}
	}
}

// processes incoming messages from the broker and writes DataRequets to the subCh
func (s *subscriber) messageHandler(client paho.Client, msg paho.Message) {
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

//func (c *MQTTConnector) discoverBrokerEndpoint() {
//	logger.Println("MQTTConnector.discoverBrokerEndpoint() discovering broker endpoint...")
//
//	backOffTime := 10 * time.Second
//	backOff := func() {
//		logger.Printf("MQTTConnector.discoverBrokerEndpoint() will retry in %v", backOffTime)
//		time.Sleep(backOffTime)
//		if backOffTime <= MQTTMaxRediscoverInterval {
//			backOffTime *= 2
//			if backOffTime > MQTTMaxRediscoverInterval {
//				backOffTime = MQTTMaxRediscoverInterval
//			}
//		}
//	}
//
//	var uri, id string
//	for {
//		if c.serviceCatalogEndpoint == "" {
//			logger.Println("MQTTConnector.discoverBrokerEndpoint() discovering Service Catalog endpoint...")
//			var err error
//			c.serviceCatalogEndpoint, err = discovery.DiscoverCatalogEndpoint(sc.DNSSDServiceType)
//			if err != nil {
//				logger.Printf("MQTTConnector.discoverBrokerEndpoint() unable to discover Service Catalog: %s", err)
//				backOff()
//				continue
//			}
//		}
//
//		scc, err := scClient.NewHTTPClient(c.serviceCatalogEndpoint, nil)
//		if err != nil {
//			logger.Printf("MQTTConnector.discoverBrokerEndpoint() error creating Service Catalog client! Stopping discovery.")
//			return
//		}
//
//		// find the specified broker
//		if c.config.DiscoverID != "" {
//			service, err := scc.Get(c.config.DiscoverID)
//			if err != nil {
//				switch err.(type) {
//				case *sc.NotFoundError:
//					logger.Printf("MQTTConnector.discoverBrokerEndpoint() could not find broker: %s", c.config.DiscoverID)
//				default:
//					logger.Printf("MQTTConnector.discoverBrokerEndpoint() error searching for %s in Service Catalog: %s", c.config.DiscoverID, err)
//				}
//				backOff()
//				continue
//			}
//			uri, id = service.APIs[sc.APITypeMQTT], service.ID
//			break
//		}
//
//		// find another broker, take first match
//		res, _, err := scc.GetMany(1, 100, &scClient.FilterArgs{"name", "equals", DNSSDServiceTypeMQTT})
//		if err != nil {
//			logger.Printf("MQTTConnector.discoverBrokerEndpoint() error searching for broker in Service Catalog: %s", err)
//			backOff()
//			continue
//		}
//		if len(res) == 0 {
//			logger.Printf("MQTTConnector.discoverBrokerEndpoint() no brokers could be discovered from Service Catalog.")
//			backOff()
//			continue
//		}
//		uri, id = res[0].APIs[sc.APITypeMQTT], res[0].ID
//		break
//	}
//
//	// make the scheme compatible to Paho
//	uri = strings.Replace(uri, "mqtt://", "tcp://", 1)
//	uri = strings.Replace(uri, "mqtts://", "ssl://", 1)
//	c.config.URI = uri
//
//	err := c.config.Validate()
//	if err != nil {
//		logger.Printf("MQTTConnector.discoverBrokerEndpoint() error validating broker configuration: %s", err)
//		return
//	}
//
//	logger.Printf("MQTTConnector.discoverBrokerEndpoint() discovered broker %s with endpoint: %s", id, uri)
//	//c.discoveryCh <- uri
//	return
//}

func (c *MQTTConnector) stop() {
	logger.Println("MQTTConnector.stop()")
	for i := range c.deviceClients {
		for _, client := range c.deviceClients[i] {
			if client.paho.IsConnected() {
				logger.Println("Disconnecting", client.config.URI)
				client.paho.Disconnect(500)
				logger.Println("Disconnected", client.config.URI)
			}
		}
	}
}

func (c *mqttClient) connect(backOff time.Duration) {
	//if c.client == nil {
	//	logger.Printf("MQTTConnector.connect() client is not configured")
	//	return
	//}
	for {
		logger.Printf("MQTTConnector.connect() connecting to the broker %v, backOff: %v\n", c.config.URI, backOff)
		time.Sleep(backOff)
		if c.paho.IsConnected() {
			break
		}
		token := c.paho.Connect()
		token.Wait()
		if token.Error() == nil {
			break
		}
		logger.Printf("MQTTConnector.connect() failed to connect: %v\n", token.Error().Error())
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

func (c *mqttClient) onConnected(client paho.Client) {
	logger.Printf("MQTTPulbisher.onConnected() connected to the broker %v", c.config.URI)

	// subscribe topic is set
	if c.subscriber.topic != "" {
		logger.Printf("MQTTPulbisher.onConnected() will subscribe to topic %s", c.subscriber.topic)
		client.Subscribe(c.subscriber.topic, c.subscriber.qos, c.subscriber.messageHandler)

		//logger.Println("MQTTPulbisher.onConnected() will (re-)subscribe to all configured SUB topics")
		//topicFilters := make(map[string]byte)
		//for topic, sub := range c.subscribers {
		//	logger.Printf("MQTTPulbisher.onConnected() will subscribe to topic %s", topic)
		//	topicFilters[topic] = sub.qos
		//}
		//client.SubscribeMultiple(topicFilters, c.messageHandler)
	} else {
		logger.Println("MQTTPulbisher.onConnected() no resources with SUB configured")
	}

	// publish buffered messages to the broker
	for len(c.offlineBufferCh) > 0 {
		resp := <-c.offlineBufferCh

		if resp.IsError {
			logger.Println("MQTTConnector.onConnected() data ERROR from agent manager:", string(resp.Payload))
			continue
		}

		token := c.paho.Publish(c.publisher.topic, c.publisher.qos, c.publisher.retained, resp.Payload)
		if WaitTimeout > 0 {
			if published := token.WaitTimeout(WaitTimeout); token.Error() != nil {
				logger.Printf("MQTTConnector.onConnected() error publishing: %s", token.Error())
				continue // Note: this payload will be lost
			} else if !published {
				logger.Println("MQTTConnector.onConnected() publish timeout. Message may be lost.")
				continue
			}
		}
		logger.Printf("MQTTConnector.onConnected() published buffered message to %s (%d/%d)", c.publisher.topic, len(c.offlineBufferCh)+1, c.config.OfflineBuffer)
	}
}

func (c *mqttClient) onConnectionLost(client paho.Client, reason error) {
	logger.Println("MQTTPulbisher.onConnectionLost() lost connection to the broker: ", reason.Error())

	//// Initialize a new client and re-connect
	//c.configureMqttConnection()
	go c.connect(0)
}

func (c *mqttClient) pahoOptions(clientID string) (*paho.ClientOptions, error) {
	options := paho.NewClientOptions().
		AddBroker(c.config.URI).
		SetClientID(clientID).
		SetCleanSession(true).
		SetConnectionLostHandler(c.onConnectionLost).
		SetOnConnectHandler(c.onConnected).
		SetAutoReconnect(false) // we take care of re-connect ourselves -> paho's MaxReconnectInterval has no effect

	// Username/password authentication
	if c.config.Username != "" {
		options.SetUsername(c.config.Username)
		options.SetPassword(c.config.Password)
	}

	// SSL/TLS
	if strings.HasPrefix(c.config.URI, "ssl") {
		tlsConfig := &tls.Config{}
		// Custom CA to auth broker with a self-signed certificate
		if c.config.CaFile != "" {
			caFile, err := ioutil.ReadFile(c.config.CaFile)
			if err != nil {
				return nil, fmt.Errorf("MQTTConnector.configureMqttConnection() ERROR: failed to read CA file %s: %s", c.config.CaFile, err)
			} else {
				tlsConfig.RootCAs = x509.NewCertPool()
				ok := tlsConfig.RootCAs.AppendCertsFromPEM(caFile)
				if !ok {
					return nil, fmt.Errorf("MQTTConnector.configureMqttConnection() ERROR: failed to parse CA certificate %s", c.config.CaFile)
				}
			}
		}
		// Certificate-based client authentication
		if c.config.CertFile != "" && c.config.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.config.CertFile, c.config.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("MQTTConnector.configureMqttConnection() ERROR: failed to load client TLS credentials: %s", err)
			} else {
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
		}

		options.SetTLSConfig(tlsConfig)
	}

	return options, nil
	//c.client = paho.NewClient(connOpts)
}
