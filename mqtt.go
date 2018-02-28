// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"code.linksmart.eu/sc/service-catalog/catalog"
	"code.linksmart.eu/sc/service-catalog/client"
	"code.linksmart.eu/sc/service-catalog/discovery"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/satori/go.uuid"
)

// MQTTConnector provides paho protocol connectivity
type MQTTConnector struct {
	config                 *MqttProtocol
	clientID               string
	client                 paho.Client
	pubCh                  chan AgentResponse
	offlineBufferCh        chan AgentResponse
	subCh                  chan<- DataRequest
	pubTopics              map[string]string
	subTopicsRvsd          map[string]string // store SUB topics "reversed" to optimize lookup in messageHandler
	serviceCatalogEndpoint string
}

var WaitTimeout time.Duration = 0 // overriden by environment variable

func newMQTTConnector(conf *Config, dataReqCh chan<- DataRequest) *MQTTConnector {
	// Check if we need to publish to paho
	config, ok := conf.Protocols[ProtocolTypeMQTT].(MqttProtocol)
	if !ok {
		return nil
	}

	// check whether paho is required at all and set pub/sub topics for each resource
	pubTopics := make(map[string]string)
	subTopicsRvsd := make(map[string]string)
	requiresMqtt := false
	for _, d := range conf.Devices {
		for _, r := range d.Resources {
			for _, p := range r.Protocols {
				if p.Type == ProtocolTypeMQTT {
					requiresMqtt = true
					rid := d.ResourceId(r.Name)
					// if pub_topic is not provided - use default /prefix/<device_name>/<resource_name>
					if p.PubTopic != "" {
						pubTopics[rid] = p.PubTopic
					} else {
						pubTopics[rid] = fmt.Sprintf("%s/%s", config.Prefix, rid)
					}
					// if sub_topic is not provided - **there will be NO** sub for this resource
					if p.SubTopic != "" {
						subTopicsRvsd[p.SubTopic] = rid
					}
				}
			}
		}
	}

	if !requiresMqtt {
		return nil
	}

	// Create and return connector
	connector := &MQTTConnector{
		config:                 &config,
		clientID:               fmt.Sprintf("%v-%v", conf.Id, uuid.NewV1()),
		pubCh:                  make(chan AgentResponse, 100), // buffer to compensate for pub latencies
		offlineBufferCh:        make(chan AgentResponse, config.OfflineBuffer),
		subCh:                  dataReqCh,
		pubTopics:              pubTopics,
		subTopicsRvsd:          subTopicsRvsd,
		serviceCatalogEndpoint: conf.ServiceCatalog.Endpoint,
	}

	return connector
}

func (c *MQTTConnector) dataInbox() chan<- AgentResponse {
	return c.pubCh
}

func (c *MQTTConnector) start() {
	logger.Println("MQTTConnector.start()")

	if c.config.Discover && c.config.URL == "" {
		logger.Println("Discovering broker endpoint...")
		err := c.discoverBrokerEndpoint()
		if err != nil {
			logger.Println("MQTTConnector.start() failed to start publisher:", err.Error())
			return
		}
	}

	// configure the mqtt client
	c.configureMqttConnection()

	// start the connection routine
	logger.Printf("MQTTConnector.start() Will connect to the broker %v\n", c.config.URL)
	go c.connect(0)

	// start the publisher routine
	go c.publisher()
}

// reads outgoing messages from the pubCh und publishes them to the broker
func (c *MQTTConnector) publisher() {
	for resp := range c.pubCh {
		logger.Debugln("MQTTConnector.publisher() message:", string(resp.Payload))
		if !c.client.IsConnected() {
			if c.config.OfflineBuffer == 0 {
				logger.Println("MQTTConnector.publisher() got data while not connected to the broker. **discarded**")
				continue
			}
			select {
			case c.offlineBufferCh <- resp:
				logger.Printf("MQTTConnector.publisher() got data while not connected to the broker. Keeping in buffer (%d/%d)", len(c.offlineBufferCh), c.config.OfflineBuffer)
			default:
				logger.Printf("MQTTConnector.publisher() got data while not connected to the broker. Buffer is full (%d/%d). **discarded**", len(c.offlineBufferCh), c.config.OfflineBuffer)
			}
			continue
		}
		if resp.IsError {
			logger.Println("MQTTConnector.publisher() data ERROR from agent manager:", string(resp.Payload))
			continue
		}
		topic := c.pubTopics[resp.ResourceId]

		token := c.client.Publish(topic, byte(MQTTDefaultQoS), false, resp.Payload)
		if WaitTimeout > 0 {
			if done := token.WaitTimeout(WaitTimeout); done && token.Error() != nil {
				logger.Printf("MQTTConnector.publisher() error publishing: %s", token.Error())
				continue // Note: this payload will be lost
			} else if !done {
				logger.Printf("MQTTConnector.publisher() publish timeout. Message may be lost.")
				continue
			}
		}
		logger.Println("MQTTConnector.publisher() published to", topic)
	}
}

// processes incoming messages from the broker and writes DataRequets to the subCh
func (c *MQTTConnector) messageHandler(client paho.Client, msg paho.Message) {
	logger.Printf("MQTTConnector.messageHandler() message received: topic: %v payload: %v\n", msg.Topic(), msg.Payload())

	rid, ok := c.subTopicsRvsd[msg.Topic()]
	if !ok {
		logger.Println("The received message doesn't match any resource's configuration **discarded**")
		return
	}

	// Send Data Request
	dr := DataRequest{
		ResourceId: rid,
		Type:       DataRequestTypeWrite,
		Arguments:  msg.Payload(),
		Reply:      nil, // there will be **no reply** on the request/command execution
	}
	logger.Printf("MQTTConnector.messageHandler() Submitting data request %#v", dr)
	c.subCh <- dr
	// no response - blocking on waiting for one
}

func (c *MQTTConnector) discoverBrokerEndpoint() error {
	if c.serviceCatalogEndpoint == "" {
		var err error
		c.serviceCatalogEndpoint, err = discovery.DiscoverCatalogEndpoint(catalog.DNSSDServiceType)
		if err != nil {
			return err
		}
	}

	scc, err := client.NewHTTPClient(c.serviceCatalogEndpoint, nil)
	if err != nil {
		return err
	}

	const (
		mainBroker = "main_broker"
		mqttKey    = "MQTT"
	)

	var uri string
	service, err := scc.Get(mainBroker)
	if err == nil {
		uri = service.APIs[mqttKey]
	} else {
		// Find another broker, take first match
		res, _, err := scc.GetMany(1, 100, &client.FilterArgs{"name", "equals", DNSSDServiceTypeMQTT})
		if err != nil {
			return err
		}
		if len(res) == 0 {
			return fmt.Errorf("cound not discover any broker")
		}
		uri = res[0].APIs[mqttKey]
	}
	logger.Printf("Discovered broker endpoint: %s", uri)

	uri = strings.Replace(uri, "mqtt://", "tcp://", 1)
	uri = strings.Replace(uri, "mqtts://", "ssl://", 1)
	c.config.URL = uri

	err = c.config.Validate()
	if err != nil {
		return err
	}
	return nil
}

func (c *MQTTConnector) stop() {
	logger.Println("MQTTConnector.stop()")
	if c.client != nil && c.client.IsConnected() {
		c.client.Disconnect(500)
	}
}

func (c *MQTTConnector) connect(backOff int) {
	if c.client == nil {
		logger.Printf("MQTTConnector.connect() client is not configured")
		return
	}
	for {
		logger.Printf("MQTTConnector.connect() connecting to the broker %v, backOff: %v sec\n", c.config.URL, backOff)
		time.Sleep(time.Duration(backOff) * time.Second)
		if c.client.IsConnected() {
			break
		}
		token := c.client.Connect()
		token.Wait()
		if token.Error() == nil {
			break
		}
		logger.Printf("MQTTConnector.connect() failed to connect: %v\n", token.Error().Error())
		if backOff == 0 {
			backOff = 10
		} else if backOff <= MQTTMaxReconnectInterval {
			backOff *= 2
			if backOff > MQTTMaxReconnectInterval {
				backOff = MQTTMaxReconnectInterval
			}
		}
	}
}

func (c *MQTTConnector) onConnected(client paho.Client) {
	logger.Printf("MQTTPulbisher.onConnected() connected to the broker %v", c.config.URL)

	// subscribe if there is at least one resource with SUB in paho protocol is configured
	if len(c.subTopicsRvsd) > 0 {
		logger.Println("MQTTPulbisher.onConnected() will (re-)subscribe to all configured SUB topics")

		topicFilters := make(map[string]byte)
		for topic, _ := range c.subTopicsRvsd {
			logger.Printf("MQTTPulbisher.onConnected() will subscribe to topic %s", topic)
			topicFilters[topic] = MQTTDefaultQoS
		}
		client.SubscribeMultiple(topicFilters, c.messageHandler)
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
		topic := c.pubTopics[resp.ResourceId]

		token := c.client.Publish(topic, byte(MQTTDefaultQoS), false, resp.Payload)
		if WaitTimeout > 0 {
			if published := token.WaitTimeout(WaitTimeout); token.Error() != nil {
				logger.Printf("MQTTConnector.onConnected() error publishing: %s", token.Error())
				continue // Note: this payload will be lost
			} else if !published {
				logger.Println("MQTTConnector.onConnected() publish timeout. Message may be lost.")
				continue
			}
		}
		logger.Printf("MQTTConnector.onConnected() published buffered message to %s (%d/%d)", topic, len(c.offlineBufferCh)+1, c.config.OfflineBuffer)
	}
}

func (c *MQTTConnector) onConnectionLost(client paho.Client, reason error) {
	logger.Println("MQTTPulbisher.onConnectionLost() lost connection to the broker: ", reason.Error())

	// Initialize a new client and re-connect
	c.configureMqttConnection()
	go c.connect(0)
}

func (c *MQTTConnector) configureMqttConnection() {
	connOpts := paho.NewClientOptions().
		AddBroker(c.config.URL).
		SetClientID(c.clientID).
		SetCleanSession(true).
		SetConnectionLostHandler(c.onConnectionLost).
		SetOnConnectHandler(c.onConnected).
		SetAutoReconnect(false) // we take care of re-connect ourselves -> paho's MaxReconnectInterval has no effect

	// Username/password authentication
	if c.config.Username != "" {
		connOpts.SetUsername(c.config.Username)
		connOpts.SetPassword(c.config.Password)
	}

	// SSL/TLS
	if strings.HasPrefix(c.config.URL, "ssl") {
		tlsConfig := &tls.Config{}
		// Custom CA to auth broker with a self-signed certificate
		if c.config.CaFile != "" {
			caFile, err := ioutil.ReadFile(c.config.CaFile)
			if err != nil {
				logger.Printf("MQTTConnector.configureMqttConnection() ERROR: failed to read CA file %s:%s\n", c.config.CaFile, err.Error())
			} else {
				tlsConfig.RootCAs = x509.NewCertPool()
				ok := tlsConfig.RootCAs.AppendCertsFromPEM(caFile)
				if !ok {
					logger.Printf("MQTTConnector.configureMqttConnection() ERROR: failed to parse CA certificate %s\n", c.config.CaFile)
				}
			}
		}
		// Certificate-based client authentication
		if c.config.CertFile != "" && c.config.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.config.CertFile, c.config.KeyFile)
			if err != nil {
				logger.Printf("MQTTConnector.configureMqttConnection() ERROR: failed to load client TLS credentials: %s\n",
					err.Error())
			} else {
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
		}

		connOpts.SetTLSConfig(tlsConfig)
	}

	c.client = paho.NewClient(connOpts)
}
