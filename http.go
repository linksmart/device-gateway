// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/codegangsta/negroni"
	"github.com/gorilla/context"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	_ "github.com/linksmart/go-sec/auth/keycloak/validator"
	"github.com/linksmart/go-sec/auth/validator"
)

// errorResponse used to serialize errors into JSON for RESTful responses
type errorResponse struct {
	Error string `json:"error"`
}

// RESTfulAPI contains all required configuration for running a RESTful API
// for device gateway
type RESTfulAPI struct {
	config         *Config
	httpConfig     *HTTPProtocolConfig
	router         *mux.Router
	dataCh         chan<- DataRequest
	commonHandlers alice.Chain
}

// Constructs a RESTfulAPI data structure
func newRESTfulAPI(conf *Config, dataCh chan<- DataRequest) (*RESTfulAPI, error) {

	// Common handlers
	commonHandlers := alice.New(
		context.ClearHandler,
	)

	// Append auth handler if enabled
	if conf.Protocols.HTTP.Auth.Enabled {
		auth := conf.Protocols.HTTP.Auth
		// Setup ticket validator
		v, err := validator.Setup(auth.Provider, auth.ProviderURL, auth.ServiceID, auth.BasicEnabled, auth.Authz)
		if err != nil {
			return nil, err
		}

		commonHandlers = commonHandlers.Append(v.Handler)
	}

	api := &RESTfulAPI{
		config:         conf,
		httpConfig:     &conf.Protocols.HTTP,
		router:         mux.NewRouter().StrictSlash(true),
		dataCh:         dataCh,
		commonHandlers: commonHandlers,
	}
	return api, nil
}

// Setup all routers, handlers and start a HTTP server (blocking call)
func (api *RESTfulAPI) start() {
	api.mountResources()

	api.router.Methods("GET").Path("/").Handler(
		api.commonHandlers.ThenFunc(api.indexHandler()))

	err := mime.AddExtensionType(".jsonld", "application/ld+json")
	if err != nil {
		logger.Println("RESTfulAPI.start() ERROR:", err.Error())
	}

	// Configure the middleware
	n := negroni.New(
		negroni.NewRecovery(),
		negroni.NewLogger(),
	)
	// Mount router
	n.UseHandler(api.router)

	// Start the listener
	addr := fmt.Sprintf("%v:%v", api.httpConfig.BindAddr, api.httpConfig.BindPort)
	logger.Printf("RESTfulAPI.start() Listening on %v", addr)
	n.Run(addr)
}

// Create a HTTP handler to serve and update dashboard configuration
func (api *RESTfulAPI) dashboardHandler(confPath string) http.HandlerFunc {
	dashboardConfPath := filepath.Join(filepath.Dir(confPath), "dashboard.json")

	return func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Content-Type", "application/json")

		if req.Method == "POST" {
			body, err := ioutil.ReadAll(req.Body)
			req.Body.Close()
			if err != nil {
				api.respondWithBadRequest(rw, err.Error())
				return
			}

			err = ioutil.WriteFile(dashboardConfPath, body, 0755)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				errData := map[string]string{"error": err.Error()}
				b, _ := json.Marshal(errData)
				rw.Write(b)
				return
			}

			rw.WriteHeader(http.StatusCreated)
			rw.Write([]byte("{}"))

		} else if req.Method == "GET" {
			data, err := ioutil.ReadFile(dashboardConfPath)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				errData := map[string]string{"error": err.Error()}
				b, _ := json.Marshal(errData)
				rw.Write(b)
				return
			}
			rw.WriteHeader(http.StatusOK)
			rw.Write(data)
		} else {
			rw.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (api *RESTfulAPI) indexHandler() http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.Write([]byte("Welcome to Device Gateway RESTful API"))
	}
}

func (api *RESTfulAPI) mountResources() {
	for _, device := range api.config.devices {
		for i := range device.Protocols {
			if device.Protocols[i].Type == HTTPProtocolType {
				protocol := device.Protocols[i].HTTP
				for _, method := range protocol.Methods {
					switch method {
					case "GET":
						api.router.Methods("GET").Path(protocol.Path).Handler(
							api.commonHandlers.ThenFunc(api.createResourceGetHandler(device.Name)))
					case "PUT":
						api.router.Methods("PUT").Path(protocol.Path).Handler(
							api.commonHandlers.ThenFunc(api.createResourcePutHandler(device.Name)))
					}
					logger.Printf("RESTfulAPI.mountResources() Added %s handler for %s: %s", method, device.Name, protocol.Path)
				}
			}
		}

	}
}

func (api *RESTfulAPI) createResourceGetHandler(resourceId string) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		logger.Printf("RESTfulAPI.createResourceGetHandler() %s %s", req.Method, req.RequestURI)

		resource, found := api.config.getDevice(resourceId)
		if !found {
			api.respondWithNotFound(rw, "Resource does not exist")
			return
		}

		rw.Header().Set("Content-Type", resource.ContentType)

		// Retrieve data
		dr := DataRequest{
			ResourceId: resourceId,
			Type:       DataRequestTypeRead,
			Arguments:  nil,
			Reply:      make(chan AgentResponse),
		}
		api.dataCh <- dr

		// Wait for the response
		repl := <-dr.Reply
		if repl.IsError {
			api.respondWithInternalServerError(rw, string(repl.Payload))
			return
		}
		rw.Write(repl.Payload)
	}
}

func (api *RESTfulAPI) createResourcePutHandler(resourceId string) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		logger.Printf("RESTfulAPI.createResourcePutHandler() %s %s", req.Method, req.RequestURI)

		// Extract PUT's body
		body, err := ioutil.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			api.respondWithBadRequest(rw, err.Error())
			return
		}

		if len(body) > 0 {
			// Resolve mediaType
			mediaType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
			if err != nil {
				api.respondWithBadRequest(rw, err.Error())
				return
			}

			// Check if mediaType is supported by resource
			resource, found := api.config.getDevice(resourceId)
			if !found {
				api.respondWithNotFound(rw, "Resource does not exist")
				return
			}
			if strings.ToLower(resource.ContentType) != mediaType {
				api.respondWithUnsupportedMediaType(rw,
					fmt.Sprintf("`%s` media type is not supported by this resource", mediaType))
				return
			}
		}

		// Submit data request
		dr := DataRequest{
			ResourceId: resourceId,
			Type:       DataRequestTypeWrite,
			Arguments:  body,
			Reply:      make(chan AgentResponse),
		}
		logger.Printf("RESTfulAPI.createResourcePutHandler() Submitting data request %#v", dr)
		api.dataCh <- dr

		// Wait for the response
		repl := <-dr.Reply
		if repl.IsError {
			api.respondWithInternalServerError(rw, string(repl.Payload))
			return
		}
		rw.WriteHeader(http.StatusAccepted)
	}
}

func (api *RESTfulAPI) respondWithNotFound(rw http.ResponseWriter, msg string) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusNotFound)
	err := &errorResponse{Error: msg}
	b, _ := json.Marshal(err)
	rw.Write(b)
}

func (api *RESTfulAPI) respondWithBadRequest(rw http.ResponseWriter, msg string) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusBadRequest)
	err := &errorResponse{Error: msg}
	b, _ := json.Marshal(err)
	rw.Write(b)
}

func (api *RESTfulAPI) respondWithUnsupportedMediaType(rw http.ResponseWriter, msg string) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusUnsupportedMediaType)
	err := &errorResponse{Error: msg}
	b, _ := json.Marshal(err)
	rw.Write(b)
}

func (api *RESTfulAPI) respondWithInternalServerError(rw http.ResponseWriter, msg string) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusInternalServerError)
	err := &errorResponse{Error: msg}
	b, _ := json.Marshal(err)
	rw.Write(b)
}
