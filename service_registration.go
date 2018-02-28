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

func registerInServiceCatalog(conf *Config) (func() error, error) {
	if conf.ServiceCatalog.Endpoint == "" {
		return func() error { return nil }, nil
	}

	serviceID := conf.Id
	if conf.Id == "" {
		serviceID = uuid.NewV4().String()
	}

	var RESTEndpoint string
	if conf.Protocols[ProtocolTypeREST] != nil {
		RESTEndpoint = conf.PublicEndpoint + conf.Protocols[ProtocolTypeREST].(RestProtocol).Location
	}

	service := catalog.Service{
		ID:          serviceID,
		Name:        "_linksmart-dgw._tcp",
		Description: conf.Description,
		APIs:        map[string]string{"REST API": RESTEndpoint},
		Docs: []catalog.Doc{{
			Description: "Documentation",
			APIs:        []string{"REST API"},
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

	stopRegistrator, err := client.RegisterServiceAndKeepalive(conf.ServiceCatalog.Endpoint, service, ticket)
	if err != nil {
		return nil, err
	}

	return stopRegistrator, nil
}
