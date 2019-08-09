// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"fmt"
	"strings"

	_ "github.com/linksmart/go-sec/auth/keycloak/obtainer"
	"github.com/linksmart/go-sec/auth/obtainer"
	"github.com/linksmart/service-catalog/catalog"
	"github.com/linksmart/service-catalog/client"
)

// TODO: register via MQTT

func registerInServiceCatalog(conf *Config, apiDiscovery <-chan string) (func() error, error) {
	if conf.ServiceCatalog.Endpoint == "" {
		return func() error { return nil }, nil
	}

	//var HTTPEndpoint, MQTTEndpoint string
	//if conf.Protocols.HTTP != nil {
	//	HTTPEndpoint = conf.Protocols.HTTP.PublicEndpoint
	//}
	//if conf.Protocols.MQTT != nil {
	//	MQTTEndpoint = conf.Protocols.MQTT.URI
	//}

	var deviceNames []string
	for _, device := range conf.devices {
		deviceNames = append(deviceNames, device.Name)
	}

	service := catalog.Service{
		ID:          conf.Id,
		Name:        "_linksmart-dgw._tcp",
		Description: conf.Description,
		//APIs:        map[string]string{catalog.APITypeHTTP: RESTEndpoint, catalog.APITypeMQTT: MQTTEndpoint},
		Docs: []catalog.Doc{{
			Description: "Documentation",
			URL:         "https://docs.linksmart.eu/display/DGW",
			Type:        "text/html",
		}},
		Meta: map[string]interface{}{
			"codename":   "DGW",
			"apiVersion": Version,
			"devices":    deviceNames,
		},
		TTL: conf.ServiceCatalog.TTL,
	}

	var ticket *obtainer.Client
	var err error
	if conf.ServiceCatalog.Auth != nil {
		// Setup ticket client
		ticket, err = obtainer.NewClient(conf.ServiceCatalog.Auth.Provider, conf.ServiceCatalog.Auth.ProviderURL, conf.ServiceCatalog.Auth.Username, conf.ServiceCatalog.Auth.Password, conf.ServiceCatalog.Auth.ServiceID)
		if err != nil {
			return nil, err
		}
	}

	stopRegistrator, updateRegistry, err := client.RegisterServiceAndKeepalive(conf.ServiceCatalog.Endpoint, service, ticket)
	if err != nil {
		return nil, err
	}

	// Update registration when an endpoint is rediscovered
	go func() {
		for uri := range apiDiscovery {
			service.APIs[catalog.APITypeMQTT] = uri
			updateRegistry(service)
		}
	}()

	dServices := devicesToServices(conf)
	for _, service := range dServices {
		_, _, err := client.RegisterServiceAndKeepalive(conf.ServiceCatalog.Endpoint, service, ticket)
		if err != nil {
			return nil, err
		}
	}

	return stopRegistrator, nil
}

func devicesToServices(conf *Config) (services []catalog.Service) {
	for _, device := range conf.devices {
		var service catalog.Service
		service.ID = conf.Id + "/" + device.Name
		service.Name = DNSSDServiceTypeDGWDevice
		service.Description = device.Description
		service.Meta = device.Meta
		service.Meta["agentType"] = device.Agent.Type
		service.Meta["contentType"] = device.ContentType
		service.TTL = conf.ServiceCatalog.TTL
		service.APIs = make(map[string]string)
		mqttIndex := 1
		mqttURIs := make(map[string]bool)
		for _, protocol := range device.Protocols {
			switch protocol.Type {
			case HTTPProtocolType:
				service.APIs["HTTP"] = conf.Protocols.HTTP.PublicEndpoint + protocol.HTTP.Path
			case MQTTProtocolType:
				_, found := mqttURIs[protocol.MQTT.Client.URI]
				if found {
					continue
				}
				mqttURIs[protocol.MQTT.Client.URI] = true

				key := strings.TrimSuffix(fmt.Sprintf("MQTT_%d", mqttIndex), "_1")
				_, found = service.APIs[key]
				if found {
					mqttIndex++
					service.APIs[fmt.Sprintf("MQTT_%d", mqttIndex)] = protocol.MQTT.Client.URI
				} else {
					service.APIs[key] = conf.Protocols.MQTT.URI
				}
			}
		}
		services = append(services, service)
	}
	return services
}
