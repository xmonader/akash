package util

import (
	"fmt"
	"net"
	"context"
)

// A type that does nothing but return a result that is already existent
type staticServiceDiscoveryAgent net.SRV

func (staticServiceDiscoveryAgent) Stop()        {}
func (staticServiceDiscoveryAgent) DiscoverNow() {}
func (ssda staticServiceDiscoveryAgent) GetClient(ctx context.Context, isHttps, secure bool) (ServiceClient, error) {
	proto := "http"
	if isHttps {
		proto = "https"
	}
	url := fmt.Sprintf("%s://%v:%v", proto, ssda.Target, ssda.Port)
	return newHttpWrapperServiceClient(isHttps, secure, url), nil
}

