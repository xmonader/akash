package operator_clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	clusterutil "github.com/ovrclk/akash/provider/cluster/util"
	ipoptypes "github.com/ovrclk/akash/provider/operator/ip_operator/types"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	"github.com/tendermint/tendermint/libs/log"
	"io"
	"net"
	"net/http"
	"time"
)

var (
	errNotImplemented = errors.New("not implemented")
)

type IPOperatorClient interface{
	GetIPAddressUsage(ctx context.Context) (ipoptypes.IPAddressUsage, error)
	ReserveIPAddress(ctx context.Context, orderID mtypes.OrderID, quantity uint) (bool, error)
	UnreserveIPAddress(ctx context.Context, orderID mtypes.OrderID) error
	GetIPAddressStatus(ctx context.Context, leaseID mtypes.LeaseID) ([]ipoptypes.LeaseIPStatus, error)
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

func (_ ipOperatorNullClient) GetIPAddressStatus(ctx context.Context, id mtypes.LeaseID) ([]ipoptypes.LeaseIPStatus, error) {
	return nil, errNotImplemented
}


func NewIPOperatorClient(logger log.Logger) (IPOperatorClient, error) {
	sda := clusterutil.NewServiceDiscoveryAgent(logger, "api", "akash-ip-operator", "akash-services", "tcp")

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
	sda clusterutil.ServiceDiscoveryAgent
	httpClient *http.Client
	log log.Logger
}

func (ipoc *ipOperatorClient) newRequest(ctx context.Context, method string, path string, body io.Reader) (*http.Request, error) {
	addr, err := ipoc.sda.GetAddress(ctx)
	if err != nil {
		return nil, err
	}
	remoteURL := fmt.Sprintf("http://%s:%d%s", addr.Target, addr.Port, path)
	return http.NewRequest(method, remoteURL, body)
}

func (ipoc *ipOperatorClient) GetIPAddressStatus(ctx context.Context, leaseID mtypes.LeaseID) ([]ipoptypes.LeaseIPStatus, error) {
	path := fmt.Sprintf("/ip-lease-status/%s/%d/%d/%d", leaseID.GetOwner(), leaseID.GetDSeq(), leaseID.GetGSeq(), leaseID.GetOSeq())
	req, err := ipoc.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	ipoc.log.Debug("asking for IP address status", "method", req.Method, "url", req.URL)
	response, err := ipoc.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	ipoc.log.Debug("ip address status request result", "status", response.StatusCode)

	if response.StatusCode == http.StatusNoContent {
		return nil, nil // No data for this lease
	}

	if response.StatusCode != http.StatusOK {
		return nil, extractRemoteError(response.Body)
	}

	var result []ipoptypes.LeaseIPStatus

	decoder := json.NewDecoder(response.Body)
	err = decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
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
	ipoc.log.Info("ip reservation request result", "status", response.StatusCode)

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
