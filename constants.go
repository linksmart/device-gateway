// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import "time"

const (
	// Use to invalidate cache during the requests for agent's data
	AgentResponseCacheTTL = 3 * time.Second

	// DNS-SD service name (type)
	DNSSDServiceTypeDGW  = "_linksmart-dgw._tcp"
	DNSSDServiceTypeMQTT = "_mqtt._tcp"

	// HTTP
	HTTPProtocolType = "HTTP"

	// MQTT
	MQTTProtocolType          = "MQTT"
	MQTTDefaultQoS            = 1
	MQTTMaxReconnectInterval  = 60 * time.Second
	MQTTMaxRediscoverInterval = 60 * time.Second
	MQTTPublishTimeoutEnvKey  = "DGW_PUBLISH_TIMEOUT"
	MQTTPubMethod             = "PUB"
	MQTTSubMethod             = "SUB"
)
