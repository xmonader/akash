package metallb

import (
	"errors"
	"fmt"
	"github.com/ovrclk/akash/provider/cluster/kube/client_common"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/flowcontrol"
	"github.com/tendermint/tendermint/libs/log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/common/expfmt"
)

type Client interface {
	GetIPAddressUsage() (uint, uint, error)
}

type client struct {
	kube kubernetes.Interface
	httpClient *http.Client

	metricsHost string
	metricsPort uint16

	log               log.Logger
}


func (c *client) String() string {
	return fmt.Sprintf("metal LB client %p (%s:%d)", c, c.metricsHost, c.metricsPort)
}

const (
	metricsPath = "/metrics"
	metricsTimeout = 10 * time.Second

	poolName = "default"

	serviceHostName = "controller.metallb-system.svc.cluster.local"

	metricNameAddrInUse = "metallb_allocator_addresses_in_use_total"
	metricNameAddrTotal = "metallb_allocator_addresses_total"
)

var (
	errMetalLB = errors.New("metal lb error")
)


func NewClient(configPath string, logger log.Logger) (Client, error){
	config, err := client_common.OpenKubeConfig(configPath, logger)
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

	_, addrs, err := net.LookupSRV("monitoring","tcp", serviceHostName)
	if err != nil {
		return nil, err
	}

	// Ignore priority & weight, just make a random selection. This generally has a length
	// of 1
	addrI := rand.Int31n(int32(len(addrs)))
	addr := addrs[addrI]

	return &client{
		kube: kc,
		httpClient: &http.Client{
			Transport:     transport,
			CheckRedirect: nil,
			Jar:           nil,
			Timeout:       metricsTimeout,
		},
		metricsHost: addr.Target,
		metricsPort: addr.Port,
		log: logger.With("client","metallb"),
	}, nil

}

/*
can get stuff like this to access metal lb metrics
   75  nslookup -type=SRV _monitoring._tcp.

  102  curl -I controller.metallb-system.svc.cluster.local:7472/metrics

 */


func (c *client) GetIPAddressUsage() (uint, uint,  error) {
	metricsURL := fmt.Sprintf("http://%s:%d%s", c.metricsHost, c.metricsPort, metricsPath)
	request, err := http.NewRequest(http.MethodGet, metricsURL, nil)
	if err != nil {
		return math.MaxUint32,math.MaxUint32, err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return math.MaxUint32,math.MaxUint32, err
	}

	if response.StatusCode != http.StatusOK {
		return math.MaxUint32,math.MaxUint32, fmt.Errorf("%w: response status %d", errMetalLB, response.StatusCode)
	}


	var parser expfmt.TextParser
	mf, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return math.MaxUint32,math.MaxUint32, err
	}

	/**
	  Loooking for the following metrics
	    metallb_allocator_addresses_in_use_total{pool="default"} 0
	    metallb_allocator_addresses_total{pool="default"} 100
	 */

	available := uint(0)
	setAvailable := false
	inUse := uint(0)
	setInUse := false
	for _, entry := range mf {
		if setInUse && setAvailable {
			break
		}
		var target *uint
		var setTarget *bool
		if entry.GetName() == metricNameAddrInUse   {
			target = &inUse
			setTarget = &setInUse
		} else if entry.GetName() == metricNameAddrTotal {
			target = &available
			setTarget = &setAvailable
		} else {
			continue
		}

		metric := entry.GetMetric()
		searchLoop:
		for _, metricEntry := range metric{
			gauge := metricEntry.GetGauge()
			if gauge == nil {
				continue
			}
			for _, labelEntry := range metricEntry.Label {
				if labelEntry.GetName()  != "pool" {
					continue
				}

				if labelEntry.GetValue() != poolName {
					continue
				}

				*target = uint(*gauge.Value)
				*setTarget = true
				break searchLoop
			}
		}
	}

	if !setInUse || !setAvailable {
		return math.MaxUint32, math.MaxUint32, fmt.Errorf("%w: data not found in metrics response", errMetalLB)
	}

	return inUse, available, nil
}

