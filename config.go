// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/linksmart/go-sec/authz"
)

//
// Loads a configuration form a given path
//
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
		return nil, err
	}

	if err = config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

//
// Main configuration struct
//
type Config struct {
	Id             string             `json:"id"` // used as service id, mqtt client id,
	Description    string             `json:"description"`
	DnssdEnabled   bool               `json:"dnssdEnabled"`
	Protocols      Protocols          `json:"protocols"`
	ServiceCatalog ServiceCatalogConf `json:"serviceCatalog"`
	devices        []Device
}

type Protocols struct {
	HTTP HttpProtocolConfig `json:"HTTP"`
	MQTT MqttProtocolConfig `json:"MQTT"`
}

// Validates the loaded configuration
func (c *Config) Validate() error {

	err := c.Protocols.HTTP.Validate()
	if err != nil {
		return err
	}

	err = c.Protocols.MQTT.Validate()
	if err != nil {
		return err
	}

	for _, device := range c.devices {
		err = device.validate()
		if err != nil {
			return fmt.Errorf("%s: %s", device.configPath, err)
		}
	}

	return nil
}

func (c *Config) findDevice(name string) (*Device, bool) {
	for i := range c.devices {
		if name == c.devices[i].Name {
			return &c.devices[i], true
		}
	}
	return nil, false
}

//
// Http config (for protocols using it)
//
type HttpProtocolConfig struct {
	PublicEndpoint string        `json:"publicEndpoint"`
	BindAddr       string        `json:"bindAddr"`
	BindPort       int           `json:"bindPort"`
	Auth           ValidatorConf `json:"auth"`
}

func (h *HttpProtocolConfig) Validate() error {
	if h.BindAddr == "" || h.BindPort == 0 {
		return fmt.Errorf("HTTP bindAddr and bindPort not set")
	}
	// Check if PublicEndpoint is valid
	if h.PublicEndpoint == "" {
		return fmt.Errorf("HTTP publicEndpoint not set")
	}
	_, err := url.Parse(h.PublicEndpoint)
	if err != nil {
		return fmt.Errorf("HTTP publicEndpoint not a valid URL")
	}

	if h.Auth.Enabled {
		// Validate ticket validator config
		err = h.Auth.Validate()
		if err != nil {
			return err
		}
	}

	return nil
}

type MqttProtocolConfig struct {
	Discover      bool   `json:"discover"`
	DiscoverID    string `json:"discoverID"`
	URI           string `json:"uri"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	CaFile        string `json:"caFile"`
	CertFile      string `json:"certFile"`
	KeyFile       string `json:"keyFile"`
	OfflineBuffer uint   `json:"offlineBuffer"`
}

func (p *MqttProtocolConfig) Validate() error {
	if !p.Discover {
		parsedURL, err := url.Parse(p.URI)
		if err != nil {
			return fmt.Errorf("MQTT broker URI must be a valid URI in the format scheme://host:port")
		}
		if parsedURL.Scheme != "tcp" && parsedURL.Scheme != "ssl" {
			return fmt.Errorf("MQTT broker URI scheme must be either 'tcp' or 'ssl'")
		}
	}

	// Check that the CA file exists
	if p.CaFile != "" {
		if _, err := os.Stat(p.CaFile); os.IsNotExist(err) {
			return fmt.Errorf("MQTT CA file %s does not exist", p.CaFile)
		}
	}

	// Check that the client certificate and key files exist
	if p.CertFile != "" || p.KeyFile != "" {
		if _, err := os.Stat(p.CertFile); os.IsNotExist(err) {
			return fmt.Errorf("MQTT client certificate file %s does not exist", p.CertFile)
		}

		if _, err := os.Stat(p.KeyFile); os.IsNotExist(err) {
			return fmt.Errorf("MQTT client key file %s does not exist", p.KeyFile)
		}
	}
	return nil
}

//
// Device information container (has one or many resources)
//
type Device struct {
	configPath  string
	Name        string
	Description string
	Meta        map[string]interface{}
	Ttl         uint // follow dgw's ttl?
	Agent       Agent
	ContentType string
	Protocols   []DeviceProtocolConfig
}

func (d *Device) validate() error {
	if strings.HasPrefix(d.Name, "/") || strings.HasSuffix(d.Name, "/") {
		return fmt.Errorf("name should not start or end with slash: %s", d.Name)
	}
	for _, protocol := range d.Protocols {

		switch strings.ToUpper(protocol.Type) {
		case MQTTProtocolType:
			mqtt := protocol.MQTTProtocol
			if mqtt.QoS > 2 {
				return fmt.Errorf("MQTT QoS should be [0-2], not %d", mqtt.QoS)
			}
			if mqtt.Client != nil {
				err := mqtt.Client.Validate()
				if err != nil {
					return fmt.Errorf("MQTT client config is invalid: %s", err)
				}
			}
		case HTTPProtocolType:
			//
		default:
			return fmt.Errorf("unknown protocol: %T", protocol.Type)
		}
	}
	return nil
}

type DeviceProtocolConfig struct {
	Type    string
	Methods []string `json:"methods"`
	*MQTTProtocol
	*HTTPProtocol
}

type MQTTProtocol struct {
	Topic    string              `json:"topic"`
	Retained bool                `json:"retained"`
	QoS      uint8               `json:"qos"`
	Client   *MqttProtocolConfig `json:"client"` // overrides default
}

type HTTPProtocol struct {
	Path string `json:"path"`
}

func (d *Device) DeviceName(name string) string {
	return fmt.Sprintf("%s/%s", d.Name, name)
}

//
// Description of how to run an agent that communicates with hardware
//
type Agent struct {
	Type     ExecType
	Interval time.Duration
	Dir      string
	Exec     string
}

type ExecType string

const (
	// Executes, outputs data, exits
	ExecTypeTask ExecType = "task"
	// Executes periodically (see Interval)
	ExecTypeTimer ExecType = "timer"
	// Constantly running and emitting output
	ExecTypeService ExecType = "service"
)

// Service Catalogs Registration Config
type ServiceCatalogConf struct {
	Discover bool          `json:"discover"`
	Endpoint string        `json:"endpoint"`
	TTL      uint          `json:"ttl"`
	Auth     *ObtainerConf `json:"auth"`
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
