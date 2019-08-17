// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"io"
	"os/exec"
	"time"
)

type DataRequestType string

const (
	DataRequestTypeRead  DataRequestType = "READ"
	DataRequestTypeWrite DataRequestType = "WRITE"
)

//
// An envelope data structure for requests of data from services
//
type DataRequest struct {
	ResourceId string
	Type       DataRequestType
	Arguments  []byte
	Reply      chan AgentResponse
}

//
// An envelope data structure for agent's data
//
type AgentResponse struct {
	ResourceId string
	Payload    []byte
	IsError    bool
	Cached     time.Time
}

//
// Manages agents, their executions and data caching and provisioning
//
type AgentManager struct {
	config         *Config
	timers         map[string]*time.Ticker
	services       map[string]*exec.Cmd
	serviceInpipes map[string]io.WriteCloser

	// Data cache that hold last readings from agents.
	dataCache map[string]AgentResponse

	// Agent responses inbox
	agentInbox chan AgentResponse

	// Data requests inbox
	dataRequestInbox chan DataRequest

	// Data upstream/publishing channel
	publishOutbox chan<- AgentResponse
}

//
// AgentManager constructor.
//
func newAgentManager(conf *Config) *AgentManager {
	manager := &AgentManager{
		config:           conf,
		timers:           make(map[string]*time.Ticker),
		services:         make(map[string]*exec.Cmd),
		serviceInpipes:   make(map[string]io.WriteCloser),
		dataCache:        make(map[string]AgentResponse),
		agentInbox:       make(chan AgentResponse),
		dataRequestInbox: make(chan DataRequest),
	}
	return manager
}

//
// Sets the data channel to upstream data from
//
func (am *AgentManager) setPublishingChannel(ch chan<- AgentResponse) {
	am.publishOutbox = ch
}

//
// Creates all agents and start listening on the inbox channels
//
func (am *AgentManager) start() {
	logger.Println("AgentManager.start()")

	for _, d := range am.config.devices {
		switch d.Agent.Type {
		case ExecTypeTimer:
			am.createTimer(d.Name, d.Agent)
		case ExecTypeTask:
			am.createTask(d.Name, d.Agent)
		case ExecTypeService:
			am.createService(d.Name, d.Agent)
		default:
			logger.Panicf("AgentManager.start() ERROR: Unexpected execution type %s for resource %s\n", d.Agent.Type, d.Name)
		}

	}

	// This is the main inboxes handling loop
	for {
		select {

		case resp := <-am.agentInbox:
			// Receive data from agents and cache it
			if resp.IsError {
				logger.Printf("AgentManager.start() ERROR: Received from %s: %s", resp.ResourceId, string(resp.Payload))
			}

			// Cache data
			am.dataCache[resp.ResourceId] = resp

			// Publish if required
			if am.publishOutbox != nil {
				device, ok := am.config.getDevice(resp.ResourceId)
				if !ok {
					continue
				}
				// Publish only if resource is task/service and exposes at least one MQTT publisher
				if device.Agent.Type == ExecTypeTimer || device.Agent.Type == ExecTypeService {
					for i := range device.Protocols {
						if device.Protocols[i].Type == MQTTProtocolType && device.Protocols[i].MQTT.pub {
							select {
							case am.publishOutbox <- resp:
								// response is sent
							default:
								logger.Printf("AgentManager.start() WARNING: publishOutbox is full. Skipping current value...")
							}
							break
						}
					}
				}
			}

		case req := <-am.dataRequestInbox:
			// Receive request from a service layer, check the cache hit and TTL.
			// If not available execute the task or return not available error for timer/service
			logger.Printf("AgentManager.start() Request for data from %s", req.ResourceId)

			device, ok := am.config.getDevice(req.ResourceId)
			if !ok {
				logger.Printf("AgentManager.start() ERROR: resource %s not found!", req.ResourceId)
				if req.Reply != nil {
					req.Reply <- AgentResponse{
						ResourceId: req.ResourceId,
						Payload:    []byte("Resource not found"),
						IsError:    true,
					}
				}
				continue
			}

			// For Write data requests
			if req.Type == DataRequestTypeWrite {
				if device.Agent.Type == ExecTypeTimer || device.Agent.Type == ExecTypeTask {
					am.executeTask(req.ResourceId, device.Agent, req.Arguments)
					// Respond only if the Reply channel is not nil
					if req.Reply != nil {
						req.Reply <- AgentResponse{
							ResourceId: req.ResourceId,
							Payload:    nil,
							IsError:    false,
						}
					}

				} else if device.Agent.Type == ExecTypeService {
					pipe, ok := am.serviceInpipes[req.ResourceId]
					if !ok {
						// Respond only if the Reply channel is not nil
						if req.Reply != nil {
							req.Reply <- AgentResponse{
								ResourceId: req.ResourceId,
								Payload:    []byte("Service input pipe not found"),
								IsError:    true,
							}
						}
						continue
					}
					pipe.Write(req.Arguments)
					_, err := pipe.Write([]byte("\n"))
					if err != nil {
						// Respond only if the Reply channel is not nil
						if req.Reply != nil {
							// failed to access stdin pipe
							req.Reply <- AgentResponse{
								ResourceId: req.ResourceId,
								Cached:     time.Now(),
								IsError:    true,
								Payload:    []byte(err.Error()),
							}
						}
						continue
					}
					// Respond only if the Reply channel is not nil
					if req.Reply != nil {
						req.Reply <- AgentResponse{
							ResourceId: req.ResourceId,
							Payload:    nil,
							IsError:    false,
						}
					}

				} else {
					logger.Printf("AgentManager.start() ERROR: Unsupported execution type %s for resource %s!", device.Agent.Type, req.ResourceId)
					// Respond only if the Reply channel is not nil
					if req.Reply != nil {
						req.Reply <- AgentResponse{
							ResourceId: req.ResourceId,
							Payload:    []byte("Unsupported execution type"),
							IsError:    true,
						}
					}
				}
				continue
			}

			// For Read data requests
			resp, ok := am.dataCache[req.ResourceId]
			if ok && (device.Agent.Type != ExecTypeTask || time.Now().Sub(resp.Cached) <= AgentResponseCacheTTL) {
				logger.Printf("AgentManager.start() Cache HIT for resource %s", req.ResourceId)
				req.Reply <- resp
				continue
			}
			if device.Agent.Type == ExecTypeTask {
				// execute task, cache data and return
				logger.Printf("AgentManager.start() Cache MISSED for resource %s", req.ResourceId)
				resp := am.executeTask(req.ResourceId, device.Agent, nil)
				am.dataCache[resp.ResourceId] = resp
				req.Reply <- resp
				continue
			}
			logger.Printf("AgentManager.start() ERROR: Data for resource %s not available!", req.ResourceId)
			req.Reply <- AgentResponse{
				ResourceId: req.ResourceId,
				Payload:    []byte("Data not available"),
				IsError:    true,
			}
		}
	}
}

//
// Stops all timers and services.
// Closes all channels.
//
func (am *AgentManager) stop() {
	logger.Println("AgentManager.stop()")

	// Stop timers
	for r, t := range am.timers {
		logger.Printf("AgentManager.stop() Stopping %s's timer...", r)
		t.Stop()
	}

	// Stop services
	for r, s := range am.services {
		logger.Printf("AgentManager.stop() Stopping %s's service...", r)
		am.stopService(s)
	}
}

//
// Returns a write only data request inbox
//
func (am *AgentManager) DataRequestInbox() chan<- DataRequest {
	return am.dataRequestInbox
}

//
// Create a timer for a given resource and configures a tick handling goroutine
//
func (am *AgentManager) createTimer(resourceId string, agent Agent) {
	if agent.Type != ExecTypeTimer {
		logger.Printf("AgentManager.createTimer() ERROR: %s is not %s but %s", resourceId, ExecTypeTimer, agent.Type)
		return
	}
	ticker := time.NewTicker(time.Duration(agent.TimerInterval) * time.Second)
	go func(rid string, a Agent) {
		for ; true; <-ticker.C {
			am.agentInbox <- am.executeTask(rid, a, nil)
		}
	}(resourceId, agent)
	am.timers[resourceId] = ticker

	logger.Printf("AgentManager.createTimer() %s", resourceId)
}

func (am *AgentManager) createTask(resourceId string, agent Agent) {
	if agent.Type != ExecTypeTask {
		logger.Printf("AgentManager.validateTask() ERROR: %s is not %s but %s", resourceId, ExecTypeTask, agent.Type)
		return
	}

	logger.Printf("AgentManager.createTask() %s", resourceId)
}

func (am *AgentManager) createService(resourceId string, agent Agent) {
	if agent.Type != ExecTypeService {
		logger.Printf("AgentManager.createService() ERROR: %s is not %s but %s", resourceId, ExecTypeService, agent.Type)
		return
	}
	service, err := am.executeService(resourceId, agent)
	if err != nil {
		logger.Printf("AgentManager.createService() ERROR: Failed to create service %s: %s", resourceId, err.Error())
		return
	}
	am.services[resourceId] = service

	logger.Printf("AgentManager.createService() %s", resourceId)
}
