// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/linksmart/go-sec/auth/obtainer"
	"github.com/linksmart/go-sec/authz"
	sc "github.com/linksmart/service-catalog/v2/catalog"
	scClient "github.com/linksmart/service-catalog/v2/client"
	uuid "github.com/satori/go.uuid"
)

//
// Main configuration struct
//
type Config struct {
	ID             string             `json:"id"` // used as service id, as part of mqtt client id
	Description    string             `json:"description"`
	DnssdEnabled   bool               `json:"dnssdEnabled"`
	Protocols      Protocols          `json:"protocols"`
	ServiceCatalog ServiceCatalogConf `json:"serviceCatalog"`
	devices        []Device
}

// Loads a configuration form a given path
func loadConfig(confPath string) (*Config, error) {
	file, err := ioutil.ReadFile(confPath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(file, &config)
	if err != nil {
		return nil, err
	}

	dir := filepath.Dir(confPath)
	devicesDir := filepath.Join(dir, "devices")
	if _, err = os.Stat(devicesDir); os.IsNotExist(err) {
		return nil, err
	}

	err = filepath.Walk(devicesDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		if err != nil {
			return err
		}

		f, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		var device Device
		err = json.Unmarshal(f, &device)
		if err != nil {
			return err
		}
		device.configPath = path
		config.devices = append(config.devices, device)

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error finding device files: %s", err)
	}
	return &config, nil
}

// Validates the loaded configuration
func (c *Config) validate() error {

	err := c.Protocols.validate()
	if err != nil {
		return fmt.Errorf("protocols: %s", err)
	}

	for _, device := range c.devices {
		err = device.validate()
		if err != nil {
			return fmt.Errorf("%s: %s", device.configPath, err)
		}
	}

	err = c.ServiceCatalog.validate()
	if err != nil {
		return fmt.Errorf("serviceCatalog: %s", err)
	}

	return nil
}

// Modifies the configuration for use by other modules
func (c *Config) revise() {
	logger.Printf("Revising configurations:")

	if c.ID == "" {
		c.ID = uuid.NewV4().String()
		logger.Printf("├─ ID not set. Generated randomly: %s", c.ID)
	}

	for di := range c.devices {
		for pi := range c.devices[di].Protocols {
			c.devices[di].Protocols[pi].Type = strings.ToUpper(c.devices[di].Protocols[pi].Type)
			switch c.devices[di].Protocols[pi].Type {
			case HTTPProtocolType:
				for hi := range c.devices[di].Protocols[pi].HTTP.Methods {
					c.devices[di].Protocols[pi].HTTP.Methods[hi] = strings.ToUpper(c.devices[di].Protocols[pi].HTTP.Methods[hi])
				}
				if c.devices[di].Protocols[pi].HTTP.Path == "" {
					path := "/" + c.devices[di].Name
					c.devices[di].Protocols[pi].HTTP.Path = path
					logger.Printf("├─ %s.protocols[%d]: HTTP path not set. Used /<device-name>: %s", c.devices[di].Name, pi, path)
				}
			case MQTTProtocolType:
				if c.devices[di].Protocols[pi].MQTT.Client == nil {
					c.devices[di].Protocols[pi].MQTT.Client = &c.Protocols.MQTT
					logger.Printf("├─ %s.protocols[%d]: MQTT client not set. Used global client: %s", c.devices[di].Name, pi, c.Protocols.MQTT.URI)
				}
			}
		}
	}
}

func (c *Config) discoverEndpoints() error {
	if c.ServiceCatalog.Endpoint == "" {
		return fmt.Errorf("cannot discover without a service catalog")
	}

	for di := range c.devices {
		for pi := range c.devices[di].Protocols {
			if c.devices[di].Protocols[pi].Type == MQTTProtocolType && c.devices[di].Protocols[pi].Client.URI == "" {
				endpoint, err := c.discoverBrokerEndpoint(c.devices[di].Protocols[pi].Client.Discover)
				if err != nil {
					return fmt.Errorf("error discovering broker endpoint: %s", err)
				}
				c.devices[di].Protocols[pi].Client.URI = endpoint
			}
		}
	}

	return nil
}

func (c *Config) getDevice(name string) (*Device, bool) {
	for i := range c.devices {
		if name == c.devices[i].Name {
			return &c.devices[i], true
		}
	}
	return nil, false
}

type Protocols struct {
	HTTP HTTPProtocolConfig `json:"HTTP"`
	MQTT MQTTProtocolConfig `json:"MQTT"`
}

func (p *Protocols) validate() error {
	err := p.HTTP.validate()
	if err != nil {
		return fmt.Errorf("HTTP: %s", err)
	}

	err = p.MQTT.validate()
	if err != nil {
		return fmt.Errorf("MQTT: %s", err)
	}

	return nil
}

type HTTPProtocolConfig struct {
	PublicEndpoint string        `json:"publicEndpoint"`
	BindAddr       string        `json:"bindAddr"`
	BindPort       int           `json:"bindPort"`
	Auth           ValidatorConf `json:"auth"`
}

func (h *HTTPProtocolConfig) validate() error {
	if h.BindAddr == "" {
		return fmt.Errorf("bindAddr not set")
	}
	if h.BindPort == 0 {
		return fmt.Errorf("bindPort not set")
	}
	if h.PublicEndpoint == "" {
		return fmt.Errorf("publicEndpoint not set")
	}
	_, err := url.Parse(h.PublicEndpoint)
	if err != nil {
		return fmt.Errorf("publicEndpoint not a valid URL")
	}

	if h.Auth.Enabled {
		err = h.Auth.Validate()
		if err != nil {
			return err
		}
	}

	return nil
}

type MQTTProtocolConfig struct {
	Discover      string `json:"discover"`
	URI           string `json:"uri"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	CaFile        string `json:"caFile"`
	CertFile      string `json:"certFile"`
	KeyFile       string `json:"keyFile"`
	OfflineBuffer uint   `json:"offlineBuffer"`
}

func (p *MQTTProtocolConfig) validate() error {

	if p.URI != "" {
		parsedURL, err := url.Parse(p.URI)
		if err != nil {
			return fmt.Errorf("invalid uri: %s", err)
		}
		if parsedURL.Scheme != "tcp" && parsedURL.Scheme != "ssl" {
			return fmt.Errorf("uri scheme must be either 'tcp' or 'ssl'")
		}
	} else if p.URI == "" && p.Discover == "" {
		return fmt.Errorf("neither uri nor discover are set")
	}

	// Check that the CA file exists
	if p.CaFile != "" {
		if _, err := os.Stat(p.CaFile); os.IsNotExist(err) {
			return fmt.Errorf("CA file %s does not exist", p.CaFile)
		}
	}

	// Check that the client certificate and key files exist
	if p.CertFile != "" || p.KeyFile != "" {
		if _, err := os.Stat(p.CertFile); os.IsNotExist(err) {
			return fmt.Errorf("client certFile %s does not exist", p.CertFile)
		}

		if _, err := os.Stat(p.KeyFile); os.IsNotExist(err) {
			return fmt.Errorf("client keyFile %s does not exist", p.KeyFile)
		}
	}
	return nil
}

type Device struct {
	configPath  string
	Name        string
	Description string
	Meta        map[string]interface{}
	Agent       Agent
	ContentType string
	Protocols   []DeviceProtocolConfig
}

func (d *Device) validate() error {
	if strings.HasPrefix(d.Name, "/") || strings.HasSuffix(d.Name, "/") {
		return fmt.Errorf("name should not start or end with slash: %s", d.Name)
	}

	err := d.Agent.validate()
	if err != nil {
		return fmt.Errorf("agent: %s", err)
	}

	for pi := range d.Protocols {
		err := d.Protocols[pi].validate()
		if err != nil {
			return fmt.Errorf("protocols[%d]: %s", pi, err)
		}
	}
	return nil
}

type DeviceProtocolConfig struct {
	Type string
	*MQTT
	*HTTP
}

func (p *DeviceProtocolConfig) validate() error {
	switch strings.ToUpper(p.Type) {
	case MQTTProtocolType:
		err := p.MQTT.validate()
		if err != nil {
			return fmt.Errorf("mqtt: %s", err)
		}
	case HTTPProtocolType:
		err := p.HTTP.validate()
		if err != nil {
			return fmt.Errorf("http: %s", err)
		}
	default:
		return fmt.Errorf("unknown type: %s", p.Type)
	}
	return nil
}

type MQTT struct {
	PubTopic    string              `json:"pubTopic"`
	PubRetained bool                `json:"pubRetained"` // default = false
	PubQoS      uint8               `json:"pubQoS"`      // default = 0
	SubTopic    string              `json:"subTopic"`
	SubQoS      uint8               `json:"subQoS"` // default = 0
	Client      *MQTTProtocolConfig `json:"client"` // overrides default
}

func (m *MQTT) validate() error {
	if m.PubTopic == "" && m.SubTopic == "" {
		return fmt.Errorf("pubTopic and subTopic are both empty")
	}
	if m.PubTopic != "" && m.SubTopic != "" && m.PubTopic == m.SubTopic {
		return fmt.Errorf("pubTopic and subTopic must not be equal")
	}
	if m.PubQoS > 2 {
		return fmt.Errorf("pubQoS should be [0-2], not %d", m.PubQoS)
	}
	if m.SubQoS > 2 {
		return fmt.Errorf("subQoS should be [0-2], not %d", m.SubQoS)
	}
	if m.Client != nil {
		err := m.Client.validate()
		if err != nil {
			return fmt.Errorf("client: %s", err)
		}
	}
	return nil
}

type HTTP struct {
	Methods []string `json:"methods"`
	Path    string   `json:"path"`
}

func (h *HTTP) validate() error {
	if len(h.Methods) == 0 {
		return fmt.Errorf("methods not set")
	}
	for _, method := range h.Methods {
		if strings.ToUpper(method) != http.MethodGet && strings.ToUpper(method) != http.MethodPut {
			return fmt.Errorf("methods should be GET or PUT, not %s", method)
		}
	}
	return nil
}

// Exec types
const (
	ExecTypeTask    = "task"    // Executes, outputs data, exits
	ExecTypeTimer   = "timer"   // Executes periodically (see Interval)
	ExecTypeService = "service" // Constantly running and emitting output
)

//
// Description of how to run an agent that communicates with hardware
//
type Agent struct {
	Type     string
	Interval time.Duration
	Dir      string
	Exec     string
}

func (a *Agent) validate() error {
	if a.Type != ExecTypeTask && a.Type != ExecTypeTimer && a.Type != ExecTypeService {
		return fmt.Errorf("invalid exec type: %s", a.Type)
	}
	if a.Exec == "" {
		return fmt.Errorf("exec command no set")
	}
	return nil
}

// TODO move to a package
// Service Catalogs Registration Config
type ServiceCatalogConf struct {
	Discover bool          `json:"discover"`
	Endpoint string        `json:"endpoint"`
	TTL      uint          `json:"ttl"`
	Auth     *ObtainerConf `json:"auth"`
}

func (c *ServiceCatalogConf) validate() error {
	if c.Endpoint != "" {
		if c.Auth != nil {
			err := c.Auth.Validate()
			if err != nil {
				return fmt.Errorf("auth: %s", err)
			}
		}
	}
	return nil
}

// Ticket Validator Config
type ValidatorConf struct {
	// Auth switch
	Enabled bool `json:"enabled"`
	// Authentication provider name
	Provider string `json:"provider"`
	// Authentication provider URL
	ProviderURL string `json:"providerURL"`
	// Service ID
	ServiceID string `json:"serviceID"`
	// Basic Authentication switch
	BasicEnabled bool `json:"basicEnabled"`
	// Authorization config
	Authz *authz.Conf `json:"authorization"`
}

func (c ValidatorConf) Validate() error {

	// Validate Provider
	if c.Provider == "" {
		return errors.New("Ticket Validator: Auth provider name (provider) is not specified.")
	}

	// Validate ProviderURL
	if c.ProviderURL == "" {
		return errors.New("Ticket Validator: Auth provider URL (providerURL) is not specified.")
	}
	_, err := url.Parse(c.ProviderURL)
	if err != nil {
		return errors.New("Ticket Validator: Auth provider URL (providerURL) is invalid: " + err.Error())
	}

	// Validate ServiceID
	if c.ServiceID == "" {
		return errors.New("Ticket Validator: Auth Service ID (serviceID) is not specified.")
	}

	// Validate Authorization
	if c.Authz != nil {
		if err := c.Authz.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// Ticket Obtainer Client Config
type ObtainerConf struct {
	// Authentication provider name
	Provider string `json:"provider"`
	// Authentication provider URL
	ProviderURL string `json:"providerURL"`
	// Service ID
	ServiceID string `json:"serviceID"`
	// User credentials
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c ObtainerConf) Validate() error {

	// Validate Provider
	if c.Provider == "" {
		return errors.New("Ticket Obtainer: Auth provider name (provider) is not specified.")
	}

	// Validate ProviderURL
	if c.ProviderURL == "" {
		return errors.New("Ticket Obtainer: Auth provider URL (ProviderURL) is not specified.")
	}
	_, err := url.Parse(c.ProviderURL)
	if err != nil {
		return errors.New("Ticket Obtainer: Auth provider URL (ProviderURL) is invalid: " + err.Error())
	}

	// Validate Username
	if c.Username == "" {
		return errors.New("Ticket Obtainer: Auth Username (username) is not specified.")
	}

	// Validate ServiceID
	if c.ServiceID == "" {
		return errors.New("Ticket Obtainer: Auth Service ID (serviceID) is not specified.")
	}

	return nil
}

// TODO move to a package
func (c *Config) discoverBrokerEndpoint(id string) (string, error) {

	logger.Printf("Discovering endpoint for broker %s...", id)

	backOffTime := 10 * time.Second
	backOff := func() {
		time.Sleep(backOffTime)
		if backOffTime <= MQTTMaxRediscoverInterval {
			backOffTime *= 2
			if backOffTime > MQTTMaxRediscoverInterval {
				backOffTime = MQTTMaxRediscoverInterval
			}
		}
	}

	var ticket *obtainer.Client
	var err error
	if c.ServiceCatalog.Auth != nil {
		// Setup ticket client
		ticket, err = obtainer.NewClient(c.ServiceCatalog.Auth.Provider, c.ServiceCatalog.Auth.ProviderURL, c.ServiceCatalog.Auth.Username, c.ServiceCatalog.Auth.Password, c.ServiceCatalog.Auth.ServiceID)
		if err != nil {
			return "", fmt.Errorf("error creating auth token obtainer for Service Catalog: %s", err)
		}
	}

	for {
		scc, err := scClient.NewHTTPClient(c.ServiceCatalog.Endpoint, ticket)
		if err != nil {
			return "", fmt.Errorf("error creating Service Catalog client: %s", err)
		}

		ok, err := scc.Ping()
		if err != nil {
			return "", fmt.Errorf("error pinging Service Catalog: %s", err)
		}
		if !ok {
			logger.Printf("Could not reach Service Catalog. Will retry in %v", backOffTime)
			backOff()
			continue
		}

		service, err := scc.Get(id)
		if err != nil {
			switch err.(type) {
			case *sc.NotFoundError:
				return "", fmt.Errorf("broker %s was not found in Service Catalog", id)
			default:
				return "", fmt.Errorf("error searching for broker %s in Service Catalog: %s", id, err)
			}

		}
		uri, found := service.APIs[sc.APITypeMQTT]
		if !found {
			return "", fmt.Errorf("unable to extract broker endpoint with key: %s", sc.APITypeMQTT)
		}
		logger.Printf("Discovered endpoint for %s: %s", id, uri)

		// make the scheme compatible to Paho
		uri = strings.Replace(uri, "mqtt://", "tcp://", 1)
		uri = strings.Replace(uri, "mqtts://", "ssl://", 1)

		return uri, nil
	}
}
