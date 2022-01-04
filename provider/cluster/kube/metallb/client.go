package metallb

import (
	"context"
	"errors"
	"fmt"
	"github.com/ovrclk/akash/manifest"
	"github.com/ovrclk/akash/provider/cluster/kube/builder"
	"github.com/ovrclk/akash/provider/cluster/kube/client_common"
	ctypes "github.com/ovrclk/akash/provider/cluster/types"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/pager"
	"k8s.io/client-go/util/flowcontrol"
	"github.com/tendermint/tendermint/libs/log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
)

const (
	akashServiceTarget = "akash.network/service-target"
	akashMetalLB = "metal-lb"
	metalLbAllowSharedIp = "metallb.universe.tf/allow-shared-ip"
)


type Client interface {
	GetIPAddressUsage() (uint, uint, error)
	GetIPAddressStatusForLease(ctx context.Context, leaseID mtypes.LeaseID) ([]ctypes.IPLeaseState, error)
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

	// TODO - if the pod is rebooted, do we need to rediscover this?
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
	ctx := context.Background() // TODO - make this method take a context
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
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

type ipLeaseState struct {
	leaseID mtypes.LeaseID
	ip string
	serviceName string
	externalPort uint32
	port uint32
	sharingKey string
	protocol manifest.ServiceProtocol

}

func (ipls ipLeaseState) GetLeaseID() mtypes.LeaseID {
	return ipls.leaseID
}
func (ipls ipLeaseState) GetIP() string {
	return ipls.ip
}
func (ipls ipLeaseState) GetServiceName() string {
	return ipls.serviceName
}
func (ipls ipLeaseState)GetExternalPort() uint32 {
	return ipls.externalPort
}
func (ipls ipLeaseState)GetPort() uint32 {
	return ipls.port
}
func (ipls ipLeaseState)GetSharingKey() string {
	return ipls.sharingKey
}
func (ipls ipLeaseState)GetProtocol() manifest.ServiceProtocol {
	return ipls.protocol
}

func (c *client) GetIPAddressStatusForLease(ctx context.Context, leaseID mtypes.LeaseID) ([]ctypes.IPLeaseState, error){
	ns := builder.LidNS(leaseID)
	servicePager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error){
		return c.kube.CoreV1().Services(ns).List(ctx, opts)
	})

	labelSelector := &strings.Builder{}

	_, err := fmt.Fprintf(labelSelector, "%s=true", builder.AkashManagedLabelName)
	if err != nil {
		return nil, err
	}
	_, err = fmt.Fprintf(labelSelector, ",%s=%s", akashServiceTarget, akashMetalLB)
	if err != nil {
		return nil, err
	}

	result := make([]ctypes.IPLeaseState, 0)
	err = servicePager.EachListItem(ctx, metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	},
		func(obj runtime.Object) error {
			service := obj.(*corev1.Service)

			loadBalancerIngress := service.Status.LoadBalancer.Ingress
			// Logs something like this : │ load balancer status                         cmp=provider client=kube service=web-ip-80-tcp lb-ingress="[{IP:24.0.0.1 Hostname: Ports:[]}]"
			c.log.Debug("load balancer status", "service", service.ObjectMeta.Name, "lb-ingress", loadBalancerIngress)

			// There is no mechanism that would assign more than one IP to a single service entry
			if len(loadBalancerIngress) != 1 {
				// TODO return an error indicating something is invalid
				panic("invalid load balancer ingress")
			}

			ingress := loadBalancerIngress[0]

			if len(service.Spec.Ports) != 1 {
				panic("invalid port specs")
			}
			port := service.Spec.Ports[0]

			// TODO - make this some sort of utility method
			var proto manifest.ServiceProtocol
			switch(port.Protocol) {

			case corev1.ProtocolTCP:
				proto = manifest.TCP
			case corev1.ProtocolUDP:
				proto = manifest.UDP
			default:
				panic("unknown proto from kube: " + string(port.Protocol))
			}


			// Note: don't care about node port here, even if it is assigned
			result = append(result, ipLeaseState{
				leaseID:      leaseID,
				ip:           ingress.IP,
				serviceName:  service.Name,
				externalPort: uint32(port.Port),
				port:         uint32(port.TargetPort.IntValue()),
				sharingKey:   service.ObjectMeta.Annotations[metalLbAllowSharedIp],
				protocol:     proto,
			})

			return nil
		})

	return result, nil
}
