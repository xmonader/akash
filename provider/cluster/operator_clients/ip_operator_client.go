package operator_clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/boz/go-lifecycle"
	"github.com/desertbit/timer"
	ipoptypes "github.com/ovrclk/akash/provider/operator/ip_operator/types"
	"github.com/ovrclk/akash/util/runner"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	"github.com/tendermint/tendermint/libs/log"
	"io"
	"math/rand"
	"net"
	"net/http"
	"time"
)

var (
	errNotImplemented = errors.New("not implemented")
	ErrShuttingDown = errors.New("shutting down")
)

type IPOperatorClient interface{
	GetIPAddressUsage(ctx context.Context) (ipoptypes.IPAddressUsage, error)
	ReserveIPAddress(ctx context.Context, orderID mtypes.OrderID, quantity uint) (bool, error)
	UnreserveIPAddress(ctx context.Context, orderID mtypes.OrderID) error
	Stop(ctx context.Context) error
}


/* A null client for use in tests and other scenarios */
type ipOperatorNullClient struct{}
func NullClient() IPOperatorClient {
	return ipOperatorNullClient{}
}

func (_ ipOperatorNullClient) GetIPAddressUsage(ctx context.Context) (ipoptypes.IPAddressUsage, error) {
	return ipoptypes.IPAddressUsage{}, errNotImplemented
}


func (_ ipOperatorNullClient) ReserveIPAddress(ctx context.Context, orderID mtypes.OrderID, quantity uint) (bool, error) {
	return false, errNotImplemented
}

func (_ ipOperatorNullClient) UnreserveIPAddress(ctx context.Context, orderID mtypes.OrderID) error {
	return errNotImplemented
}

func (_ ipOperatorNullClient) Stop(ctx context.Context) error {
	return nil
}


func NewServiceDiscoveryAgent(logger log.Logger, portName, serviceName, namespace, protocol string) *ServiceDiscoveryAgent {
	sda := &ServiceDiscoveryAgent{
		serviceName:     serviceName,
		namespace:       namespace,
		portName:        portName,
		portProtocol:    protocol,
		lc:              lifecycle.New(),
		discoverch:      make(chan struct{}, 1),
		requests:        make(chan serviceDiscoveryRequest),
		pendingRequests: nil,
		result:          nil,
		log: logger.With("cmp","service-discovery-agent"),
	}

	go sda.run()

	return sda
}

type ServiceDiscoveryAgent struct {
	serviceName string
	namespace string
	portName string
	portProtocol string
	lc lifecycle.Lifecycle

	discoverch chan struct{}

	requests chan serviceDiscoveryRequest
	pendingRequests []serviceDiscoveryRequest
	result []net.SRV
	log log.Logger
}

type serviceDiscoveryRequest struct {
	errCh chan <- error
	resultCh chan <- []net.SRV
}

func (sda *ServiceDiscoveryAgent) Stop(ctx context.Context) error {
	sda.lc.Shutdown(nil)

	select {
	case <-sda.lc.Done():
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (sda *ServiceDiscoveryAgent) DiscoverNow() {
	select {
	case sda.discoverch <- struct{}{}:
	default:
	}
}

func (sda *ServiceDiscoveryAgent) run(){
	defer sda.lc.ShutdownCompleted()
	addrs := make([]net.SRV, 0)

	const retryInterval = time.Second * 2
	retryTimer := timer.NewTimer(retryInterval)
	retryTimer.Stop()
	defer retryTimer.Stop()
	var discoveryResult <- chan runner.Result

	mainLoop:
	for {
		discover := len(addrs) == 0
		select {
		case <- sda.lc.ShutdownRequest():
			break mainLoop
		case <- sda.discoverch:
			discover = true // Could be ignored if discoveryResult is not nil
		case <- retryTimer.C:
			retryTimer.Stop()
			discover = true
		case result := <- discoveryResult:
			err := result.Error()
			if err != nil {
				sda.setResult(nil, err)
				retryTimer.Reset(retryInterval)
				break
			}

			addrs = (result.Value()).([]net.SRV)
			sda.setResult(addrs, nil)
		case req := <- sda.requests:
			sda.handleRequest(req)
		}

		if discover && discoveryResult == nil{
			discoveryResult = runner.Do(func() runner.Result{
				return runner.NewResult(sda.discover())
			})
		}
	}
}

func (sda *ServiceDiscoveryAgent) handleRequest(req serviceDiscoveryRequest) {
	if sda.result != nil {
		v := append([]net.SRV{}, sda.result...)
		req.resultCh <- v
		return
	}

	sda.pendingRequests = append(sda.pendingRequests, req)
}

func (sda *ServiceDiscoveryAgent) setResult(addrs []net.SRV, err error){
	for _, pendingRequest := range sda.pendingRequests {
		if err == nil {
			v := append([]net.SRV{}, addrs...)
			pendingRequest.resultCh <- v
		} else {
			pendingRequest.errCh <- err
		}
	}

	sda.pendingRequests = nil
	if err == nil {
		sda.result = addrs
	} else {
		sda.result = nil
	}
}

func (sda *ServiceDiscoveryAgent) GetResult(ctx context.Context) ([]net.SRV, error){
	errCh := make(chan error, 1)
	resultCh := make(chan []net.SRV, 1)
	req := serviceDiscoveryRequest{
		errCh: errCh,
		resultCh: resultCh,
	}

	select {
	case sda.requests <- req:
	case <- sda.lc.ShutdownRequest():
		return nil, ErrShuttingDown
	case <- ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case result := <- resultCh:
		return result, nil
	case err := <- errCh:
		return nil, err
	case <- ctx.Done():
		return nil, ctx.Err()
	}
}

func (sda *ServiceDiscoveryAgent) GetAddresss(ctx context.Context) (net.SRV, error) {
	addrs, err := sda.GetResult(ctx)
	if err != nil {
		return net.SRV{}, err
	}
	// Ignore priority & weight, just make a random selection. This generally has a length
	// of 1
	addrI := rand.Int31n(int32(len(addrs)))
	addr := addrs[addrI]

	return addr, nil
}

func (sda *ServiceDiscoveryAgent) discover() ([]net.SRV, error){
	_, addrs, err := net.LookupSRV(sda.portName, sda.portProtocol, fmt.Sprintf("%s.%s.svc.cluster.local", sda.serviceName, sda.namespace))
	if err != nil {
		sda.log.Error("discovery failed","error", err, "portName", sda.portName, "protocol", sda.portProtocol, "service-name", sda.serviceName, "namespace", sda.namespace)
		return nil, err
	}

	// De-pointerize result
	result := make([]net.SRV, len(addrs))
	for i, addr := range addrs {
		result[i] = *addr
	}
	sda.log.Info("discovery success", "addrs", result,  "portName", sda.portName, "protocol", sda.portProtocol, "service-name", sda.serviceName, "namespace", sda.namespace)

	return result, nil
}



func NewIPOperatorClient(logger log.Logger) (IPOperatorClient, error) {
	sda := NewServiceDiscoveryAgent(logger, "api", "akash-ip-operator", "akash-services", "tcp")

	const requestTimeout = time.Second * 30 // TODO - configurable
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

	return &ipOperatorClient{
		sda: sda,
		httpClient: &http.Client{
			Transport:     transport,
			CheckRedirect: nil,
			Jar:           nil,
			Timeout:       requestTimeout,
		},
		log: logger.With("operator","ip"),
	}, nil
}

func (ipoc ipOperatorClient) Stop(ctx context.Context) error {
	return ipoc.sda.Stop(ctx)
}

const (
	ipOperatorReservationsPath = "/reservations"
)

/* A client to talk to the Akash implementation of the IP Operator via HTTP */
type ipOperatorClient struct {
	sda *ServiceDiscoveryAgent
	httpClient *http.Client
	log log.Logger
}

func (ipoc *ipOperatorClient) newRequest(ctx context.Context, method string, path string, body io.Reader) (*http.Request, error) {
	addr, err := ipoc.sda.GetAddresss(ctx)
	if err != nil {
		return nil, err
	}
	remoteURL := fmt.Sprintf("http://%s:%d%s", addr.Target, addr.Port, path)
	return http.NewRequest(method, remoteURL, body)
}

func (ipoc *ipOperatorClient) GetIPAddressUsage(ctx context.Context) (ipoptypes.IPAddressUsage, error) {
	return ipoptypes.IPAddressUsage{}, errNotImplemented
}

func (ipoc *ipOperatorClient) ReserveIPAddress(ctx context.Context, orderID mtypes.OrderID, quantity uint) (bool, error) {
	reqBody := ipoptypes.IPReservationRequest{
		OrderID:  orderID,
		Quantity: quantity,
	}
	buf := &bytes.Buffer{}
	encoder := json.NewEncoder(buf)
	err := encoder.Encode(reqBody)
	if err != nil {
		return false, err
	}
	req, err := ipoc.newRequest(ctx, http.MethodPost, ipOperatorReservationsPath, buf)
	if err != nil {
		return false, err
	}

	ipoc.log.Info("making IP reservation HTTP request", "method", req.Method, "url", req.URL)
	response, err := ipoc.httpClient.Do(req)
	if err != nil {
		ipoc.log.Error("http request failed", "err", err)
		ipoc.sda.DiscoverNow()
		return false, err
	}

	if response.StatusCode != http.StatusOK {
		return false, extractRemoteError(response.Body)
	}

	reserved := false
	dec := json.NewDecoder(response.Body)
	err = dec.Decode(&reserved)
	if err != nil {
		return false, err
	}

	return reserved, nil
}

func extractRemoteError(reader io.Reader) error{
	body := ipoptypes.IPOperatorErrorResponse{}
	decoder := json.NewDecoder(reader)
	err := decoder.Decode(&body)
	if err != nil {
		return err
	}

	if 0 == len(body.Error) {
		return io.EOF
	}

	if body.Code > 0 {
		return ipoptypes.LookupError(body.Code)
	}

	return errors.New(body.Error)
}

func (ipoc *ipOperatorClient) UnreserveIPAddress(ctx context.Context, orderID mtypes.OrderID) error {
	reqBody := ipoptypes.IPReservationDelete{
		OrderID: orderID,
	}

	buf := &bytes.Buffer{}
	encoder := json.NewEncoder(buf)
	err := encoder.Encode(reqBody)
	if err != nil {
		return err
	}

	req, err := ipoc.newRequest(ctx, http.MethodDelete, ipOperatorReservationsPath, buf)
	if err != nil {
		return err
	}

	ipoc.log.Info("making IP unreservation HTTP request", "method", req.Method, "url", req.URL)
	response, err := ipoc.httpClient.Do(req)
	if err != nil {
		ipoc.log.Error("http request failed", "err", err)
		ipoc.sda.DiscoverNow()
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		return extractRemoteError(response.Body)
	}

	return nil
}
