package metallb

import (
	"errors"
	"fmt"
	"github.com/ovrclk/akash/provider/cluster/kube/client_common"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/flowcontrol"
	"github.com/tendermint/tendermint/libs/log"
	"math"
	"net"
	"net/http"
	"time"
)

type Client interface {
	GetIPAddressCount() (uint, error)
	GetIPAddressInUseCount() (uint, error)
}

type client struct {
	kube kubernetes.Interface
	httpClient *http.Client
}


const (
	metricsPath = "/metrics"
	metricsTimeout = 10 * time.Second
)

var (
	errMetalLB = errors.New("metal lb error")
)


func NewClient(configPath string, log log.Logger) (Client, error){
	config, err := client_common.OpenKubeConfig(configPath, log)
	if err != nil {
		return nil, fmt.Errorf("%w: creating kubernetes client", err)
	}
	config.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()

	kc, err := kubernetes.NewForConfig(config)

	if err != nil {
		return nil, fmt.Errorf("%w: creating kubernetes client", err)
	}


	dialer := net.Dialer{
		Timeout:       metricsTimeout,
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
		ResponseHeaderTimeout:  metricsTimeout,
		ExpectContinueTimeout:  metricsTimeout,
		TLSNextProto:           nil,
		ProxyConnectHeader:     nil,
		GetProxyConnectHeader:  nil,
		MaxResponseHeaderBytes: 0,
		WriteBufferSize:        0,
		ReadBufferSize:         0,
		ForceAttemptHTTP2:      false,
	}

	return &client{
		kube: kc,
		httpClient: &http.Client{
			Transport:     transport,
			CheckRedirect: nil,
			Jar:           nil,
			Timeout:       metricsTimeout,
		},
	}, nil

}

/*
can get stuff like this to access metal lb metrics
   75  nslookup -type=SRV _monitoring._tcp.controller.metallb-system.svc.cluster.local

  102  curl -I controller.metallb-system.svc.cluster.local:7472/metrics

 */


func (c *client) GetIPAddressCount() (uint, error) {

	request, err := http.NewRequest(http.MethodGet, metricsPath, nil)
	if err != nil {
		return math.MaxUint32, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return math.MaxUint32, err
	}

	if response.Code != http.StatusOK {
		return math.MaxUint32, fmt.Errorf("%w: response status %d", errMetalLB, response.Code)
	}





	return 0, nil
}

func (c *client) GetIPAddressInUseCount() (uint, error) {
	return 0, nil
}
