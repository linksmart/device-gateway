package resource

import (
	"fmt"
	"net/url"
	"time"
)

// STRUCTS

// Device
type Device struct {
	Id          string                 `json:"id"`
	URL         string                 `json:"url"`
	Type        string                 `json:"type"`
	Name        string                 `json:"name,omitempty"`
	Meta        map[string]interface{} `json:"meta,omitempty"`
	Description string                 `json:"description,omitempty"`
	Ttl         *int                   `json:"ttl,omitempty"`
	Created     time.Time              `json:"created"`
	Updated     time.Time              `json:"updated"`
	Expires     *time.Time             `json:"expires,omitempty"`
	Resources   Resources              `json:"resources"`
}

// Device with only IDs of resources
type SimpleDevice struct {
	*Device
	Resources []string `json:"resources"`
}

type Resources []Resource

// Resource
type Resource struct {
	Id             string                 `json:"id"`
	URL            string                 `json:"url"`
	Type           string                 `json:"type"`
	Name           string                 `json:"name,omitempty"`
	Meta           map[string]interface{} `json:"meta,omitempty"`
	Protocols      []Protocol             `json:"protocols"`
	Representation map[string]interface{} `json:"representation,omitempty"`
	Device         string                 `json:"device"` // URL of device
}

// Protocol describes the resource API
type Protocol struct {
	Type         string                 `json:"type"`
	Endpoint     map[string]interface{} `json:"endpoint"`
	Methods      []string               `json:"methods"`
	ContentTypes []string               `json:"content-types"`
}

// Validates the Device configuration
func (d *Device) validate() error {

	_, err := url.Parse(d.Id)
	if err != nil {
		return fmt.Errorf("Device id %s cannot be used in a URL: %s", d.Id, err)
	}

	if d.Ttl != nil && *d.Ttl <= 0 {
		d.Ttl = nil
	}

	// validate all resources
	for _, r := range d.Resources {
		if err := r.validate(); err != nil {
			return err
		}
	}

	return nil
}

// Validates the Resource configuration
func (r *Resource) validate() error {
	_, err := url.Parse(r.Id)
	if err != nil {
		return fmt.Errorf("Resource id %s cannot be used in a URL: %s", r.Id, err)
	}

	return nil
}

// INTERFACES

// Controller interface
type CatalogController interface {
	// Devices
	add(d *Device) error
	get(id string) (*SimpleDevice, error)
	update(id string, d *Device) error
	delete(id string) error
	list(page, perPage int) ([]SimpleDevice, int, error)
	filter(path, op, value string, page, perPage int) ([]SimpleDevice, int, error)
	total() (int, error)
	cleanExpired(d time.Duration)

	// Resources
	getResource(id string) (*Resource, error)
	listResources(page, perPage int) ([]Resource, int, error)
	filterResources(path, op, value string, page, perPage int) ([]Resource, int, error)
	totalResources() (int, error)
}

// Storage interface
type CatalogStorage interface {
	add(d *Device) error
	update(id string, d *Device) error
	delete(id string) error
	get(id string) (*Device, error)
	list(page, perPage int) ([]Device, int, error)
	total() (int, error)
	Close() error
}
