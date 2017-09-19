// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package service

import (
	"fmt"
	"sync"
	"time"

	"code.linksmart.eu/com/go-sec/auth/obtainer"
	discovery "code.linksmart.eu/sc/service-catalog/discovery"
)

const (
	keepaliveRetries = 5
)

// Registers service given a configured Catalog Client
func RegisterService(client CatalogClient, s *Service) error {
	err := client.Update(s.Id, s)
	if err != nil {
		switch err.(type) {
		case *NotFoundError:
			// If not in the catalog - add
			_, err = client.Add(s)
			if err != nil {
				logger.Printf("RegisterService() Error adding registration: %v", err)
				return err
			}
			logger.Printf("RegisterService() Added Service registration %v", s.Id)
		default:
			logger.Printf("RegisterService() Error updating registration: %v", err)
			return err
		}
	} else {
		logger.Printf("RegisterService() Updated Service registration %v", s.Id)
	}
	return nil
}

// Registers service in the remote catalog
// endpoint: catalog endpoint. If empty - will be discovered using DNS-SD
// s: service registration
// sigCh: channel for shutdown signalisation from upstream
// ticket: set to nil for no auth
func RegisterServiceWithKeepalive(endpoint string, discover bool, s Service,
	sigCh <-chan bool, wg *sync.WaitGroup, ticket *obtainer.Client) {
	defer wg.Done()
	var err error
	if discover {
		endpoint, err = discovery.DiscoverCatalogEndpoint(DNSSDServiceType)
		if err != nil {
			logger.Printf("RegisterServiceWithKeepalive() ERROR: Failed to discover the endpoint: %v", err.Error())
			return
		}
	}

	// Configure client
	client, err := NewRemoteCatalogClient(endpoint, ticket)
	if err != nil {
		logger.Printf("RegisterServiceWithKeepalive() ERROR: Failed to create remote-catalog client: %v", err.Error())
		return
	}

	// Will not keepalive registration with a negative TTL
	if s.Ttl <= 0 {
		logger.Println("RegisterServiceWithKeepalive() WARNING: Registration has ttl <= 0. Will not start the keepalive routine")
		RegisterService(client, &s)

		// catch a shutdown signal from the upstream
		for _ = range sigCh {
			logger.Printf("RegisterServiceWithKeepalive() Removing the registration %v/%v...", endpoint, s.Id)
			client.Delete(s.Id)
			if ticket != nil {
				err := ticket.Delete()
				if err != nil {
					logger.Printf("RegisterServiceWithKeepalive() Error while deleting the TGT: %v", err.Error())
				}
			}
			return
		}
	}
	logger.Printf("RegisterServiceWithKeepalive() Will register and update registration periodically: %v/%v", endpoint, s.Id)

	// Configure & start the keepalive routine
	ksigCh := make(chan bool)
	kerrCh := make(chan error)
	go keepAlive(client, &s, ksigCh, kerrCh)

	for {
		select {
		// catch an error from the keepAlive routine
		case e := <-kerrCh:
			logger.Println("RegisterServiceWithKeepalive() ERROR:", e)
			// Re-discover the endpoint if needed and start over
			if discover {
				endpoint, err = discovery.DiscoverCatalogEndpoint(DNSSDServiceType)
				if err != nil {
					logger.Println("RegisterServiceWithKeepalive() ERROR:", err.Error())
					return
				}
			}
			logger.Println("RegisterServiceWithKeepalive() Will use the new endpoint: ", endpoint)
			client, err := NewRemoteCatalogClient(endpoint, ticket)
			if err != nil {
				logger.Printf("RegisterServiceWithKeepalive() ERROR: Failed to create remote-catalog client: %v", err.Error())
				return
			}
			go keepAlive(client, &s, ksigCh, kerrCh)

		// catch a shutdown signal from the upstream
		case <-sigCh:
			logger.Printf("RegisterServiceWithKeepalive() Removing the registration %v/%v...", endpoint, s.Id)
			// signal shutdown to the keepAlive routine & close channels
			select {
			case ksigCh <- true:
				// delete entry in the remote catalog
				client.Delete(s.Id)
				if ticket != nil {
					err := ticket.Delete()
					if err != nil {
						logger.Printf("RegisterServiceWithKeepalive() Error while deleting the TGT: %v", err.Error())
					}
				}
			case <-time.After(1 * time.Second):
				logger.Printf("RegisterDeviceWithKeepalive(): timeout removing registration %v/%v: catalog unreachable", endpoint, s.Id)
			}

			close(ksigCh)
			close(kerrCh)
			return
		}
	}
}

// Keep a given registration alive
// client: configured client for the remote catalog
// s: registration to be kept alive
// sigCh: channel for shutdown signalisation from upstream
// errCh: channel for error signalisation to upstream
func keepAlive(client CatalogClient, s *Service, sigCh <-chan bool, errCh chan<- error) {
	dur := (time.Duration(s.Ttl) * time.Second) / 2
	ticker := time.NewTicker(dur)
	errTries := 0

	// Register
	RegisterService(client, s)

	for {
		select {
		case <-ticker.C:
			err := client.Update(s.Id, s)
			if err != nil {
				switch err.(type) {
				case *NotFoundError:
					// If not in the catalog - add
					logger.Printf("keepAlive() ERROR: Registration %v not found in the remote catalog. TTL expired?", s.Id)
					_, err = client.Add(s)
					if err != nil {
						logger.Printf("keepAlive() Error adding registration: %v", err)
						errTries += 1
					} else {
						logger.Printf("keepAlive() Added Service registration %v", s.Id)
						errTries = 0
					}
				default:
					logger.Printf("keepAlive() Error updating registration: %v", err)
					errTries += 1
				}
			} else {
				logger.Printf("keepAlive() Updated Service registration %v", s.Id)
				errTries = 0
			}

			if errTries >= keepaliveRetries {
				errCh <- fmt.Errorf("Number of retries exceeded")
				ticker.Stop()
				return
			}
		case <-sigCh:
			// logger.Println("keepAlive routine shutdown signalled by the upstream")
			return
		}
	}
}
