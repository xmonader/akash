package operatorclients

import (
	"context"
	"fmt"
	clusterutil "github.com/ovrclk/akash/provider/cluster/util"
	"github.com/tendermint/tendermint/libs/log"
	"io"
	"net"
	"net/http"
	"time"
)

const (
	hostnameOperatorHealthPath = "/health"
)

type HostnameOperatorClient interface {
	Check(ctx context.Context) error
	String() string

	Stop()
}

type hostnameOperatorClient struct {
	sda        clusterutil.ServiceDiscoveryAgent
	httpClient *http.Client
	log        log.Logger
}

func NewHostnameOperatorClient(logger log.Logger) HostnameOperatorClient {
	sda := clusterutil.NewServiceDiscoveryAgent(logger, "status", "akash-hostname-operator", "akash-services", "tcp")

	dialer := net.Dialer{
		Timeout:       requestTimeout,
		Deadline:      time.Time{},
		LocalAddr:     nil,
		FallbackDelay: 0,
		KeepAlive:     0,
		Resolver:      nil,
		Control:       nil,
	}

	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            dialer.DialContext,
		DialTLSContext:         nil,
		TLSClientConfig:        nil,
		TLSHandshakeTimeout:    0,
		DisableKeepAlives:      false,
		DisableCompression:     true,
		MaxIdleConns:           1,
		MaxIdleConnsPerHost:    1,
		MaxConnsPerHost:        1,
		IdleConnTimeout:        0,
		ResponseHeaderTimeout:  requestTimeout,
		ExpectContinueTimeout:  requestTimeout,
		TLSNextProto:           nil,
		ProxyConnectHeader:     nil,
		GetProxyConnectHeader:  nil,
		MaxResponseHeaderBytes: 0,
		WriteBufferSize:        0,
		ReadBufferSize:         0,
		ForceAttemptHTTP2:      false,
	}

	return &hostnameOperatorClient{
		log: logger.With("operator", "hostname"),
		sda: sda,
		httpClient: &http.Client{
			Transport:     transport,
			CheckRedirect: nil,
			Jar:           nil,
			Timeout:       requestTimeout,
		},
	}

}

func (hopc *hostnameOperatorClient) newRequest(ctx context.Context, method string, path string, body io.Reader) (*http.Request, error) {
	// TODO - can this code path be shared with the IP operator client ?
	addr, err := hopc.sda.GetAddress(ctx)
	if err != nil {
		return nil, err
	}
	remoteURL := fmt.Sprintf("http://%s:%d%s", addr.Target, addr.Port, path)
	return http.NewRequest(method, remoteURL, body)
}

func (hopc *hostnameOperatorClient) Check(ctx context.Context) error {
	// TODO - can this code path be shared with the IP operator client ?
	req, err := hopc.newRequest(ctx, http.MethodGet, hostnameOperatorHealthPath, nil)
	if err != nil {
		return err
	}

	response, err := hopc.httpClient.Do(req)
	if err != nil {
		return err
	}
	hopc.log.Info("check result", "status", response.StatusCode)

	if response.StatusCode != http.StatusOK {
		return errNotAlive
	}

	return nil
}

func (hopc *hostnameOperatorClient) String() string {
	return fmt.Sprintf("<%T %p>", hopc, hopc)
}
func (hopc *hostnameOperatorClient) Stop() {
	hopc.sda.Stop()
}
