package util

import (
	"github.com/boz/go-lifecycle"
	"github.com/desertbit/timer"
	"github.com/ovrclk/akash/util/runner"
	"net"
	"time"
	"context"
	"github.com/tendermint/tendermint/libs/log"
	"errors"
	"fmt"
	"math/rand"
)

var (
	ErrShuttingDown = errors.New("shutting down")
)

type ServiceDiscoveryAgent interface{
	Stop()

	GetAddress(ctx context.Context) (net.SRV, error)
	DiscoverNow()
}

func NewServiceDiscoveryAgent(logger log.Logger, portName, serviceName, namespace, protocol string) ServiceDiscoveryAgent {
	sda := &serviceDiscoveryAgent{
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

type serviceDiscoveryAgent struct {
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

func (sda *serviceDiscoveryAgent) Stop()  {
	sda.lc.Shutdown(nil)
}

func (sda *serviceDiscoveryAgent) DiscoverNow() {
	select {
	case sda.discoverch <- struct{}{}:
	default:
	}
}

func (sda *serviceDiscoveryAgent) run(){
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

func (sda *serviceDiscoveryAgent) handleRequest(req serviceDiscoveryRequest) {
	if sda.result != nil {
		v := append([]net.SRV{}, sda.result...)
		req.resultCh <- v
		return
	}

	sda.pendingRequests = append(sda.pendingRequests, req)
}

func (sda *serviceDiscoveryAgent) setResult(addrs []net.SRV, err error){
	sda.log.Debug("satisfying pending requests", "qty", len(sda.pendingRequests))
	for _, pendingRequest := range sda.pendingRequests {
		if err == nil {
			v := append([]net.SRV{}, addrs...)
			pendingRequest.resultCh <- v
		} else {
			pendingRequest.errCh <- err
		}
	}

	sda.pendingRequests = nil // Clear pending requests
	if err == nil {
		sda.result = addrs
	} else {
		sda.result = nil
	}
}

func (sda *serviceDiscoveryAgent) GetResult(ctx context.Context) ([]net.SRV, error){
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

func (sda *serviceDiscoveryAgent) GetAddress(ctx context.Context) (net.SRV, error) {
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

func (sda *serviceDiscoveryAgent) discover() ([]net.SRV, error){
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

