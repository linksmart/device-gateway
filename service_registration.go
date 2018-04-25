// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	_ "code.linksmart.eu/com/go-sec/auth/keycloak/obtainer"
	"code.linksmart.eu/com/go-sec/auth/obtainer"
	"code.linksmart.eu/sc/service-catalog/catalog"
	"code.linksmart.eu/sc/service-catalog/client"
	"github.com/satori/go.uuid"
)

// TODO: register via MQTT

func registerInServiceCatalog(conf *Config, apiDiscovery <-chan string) (func() error, error) {
	if conf.ServiceCatalog.Endpoint == "" {
		return func() error { return nil }, nil
	}

	serviceID := conf.Id
	if conf.Id == "" {
		serviceID = uuid.NewV4().String()
	}

	var RESTEndpoint, MQTTEndpoint string
	if conf.Protocols[ProtocolTypeREST] != nil {
		RESTEndpoint = conf.PublicEndpoint + conf.Protocols[ProtocolTypeREST].(RestProtocol).Location
	}
	if conf.Protocols[ProtocolTypeMQTT] != nil {
		MQTTEndpoint = conf.Protocols[ProtocolTypeMQTT].(MqttProtocol).URL
	}

	service := catalog.Service{
		ID:          serviceID,
		Name:        "_linksmart-dgw._tcp",
		Description: conf.Description,
		APIs:        map[string]string{catalog.APITypeHTTP: RESTEndpoint, catalog.APITypeMQTT: MQTTEndpoint},
		Docs: []catalog.Doc{{
			Description: "Documentation",
			URL:         "http://doc.linksmart.eu/DGW",
			Type:        "text/html",
		}},
		Meta: map[string]interface{}{
			"ls_codename": "DGW",
			"api_version": Version,
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

	return stopRegistrator, nil
}
