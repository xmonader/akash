package operator_clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/avast/retry-go"
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
	Check(ctx context.Context) error
	GetIPAddressUsage(ctx context.Context) (ipoptypes.IPAddressUsage, error)

	GetIPAddressStatus(ctx context.Context, orderID mtypes.OrderID) ([]ipoptypes.LeaseIPStatus, error)
	Stop()
	String() string
}


/* A null client for use in tests and other scenarios */
type ipOperatorNullClient struct{}
func NullClient() IPOperatorClient {
	return ipOperatorNullClient{}
}

func (v ipOperatorNullClient) String() string {
	return fmt.Sprintf("<%T>", v)
}

func (_ ipOperatorNullClient) Check(ctx context.Context) (error) {
	return errNotImplemented
}

func (_ ipOperatorNullClient) GetIPAddressUsage(ctx context.Context) (ipoptypes.IPAddressUsage, error) {
	return ipoptypes.IPAddressUsage{}, errNotImplemented
}


func (_ ipOperatorNullClient) Stop(){}

func (_ ipOperatorNullClient) GetIPAddressStatus(context.Context, mtypes.OrderID) ([]ipoptypes.LeaseIPStatus, error) {
	return nil, errNotImplemented
}


func NewIPOperatorClient(logger log.Logger) (IPOperatorClient, error) {
	sda := clusterutil.NewServiceDiscoveryAgent(logger, "api", "akash-ip-operator", "akash-services", "tcp")

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
	}, nil // TODO - can we possibly return an error here?
}

func (ipoc *ipOperatorClient) String() string {
	return fmt.Sprintf("<%T %p>", ipoc, ipoc)
}

func (ipoc *ipOperatorClient) Stop() {
	ipoc.sda.Stop()
}

const (
	ipOperatorReservationsPath = "/reservations"
	ipOperatorHealthPath = "/health"
)

/* A client to talk to the Akash implementation of the IP Operator via HTTP */
type ipOperatorClient struct {
	sda clusterutil.ServiceDiscoveryAgent
	httpClient *http.Client
	log log.Logger
}

var errNotAlive = errors.New("ip operator is not yet alive")

func (ipoc *ipOperatorClient) Check(ctx context.Context) error {
	req, err := ipoc.newRequest(ctx, http.MethodGet, ipOperatorHealthPath, nil)
	if err != nil {
		return err
	}

	response, err := ipoc.httpClient.Do(req)
	if err != nil {
		return err
	}
	ipoc.log.Info("check result", "status", response.StatusCode)

	if response.StatusCode != http.StatusOK {
		return errNotAlive
	}

	return nil
}


func (ipoc *ipOperatorClient) newRequest(ctx context.Context, method string, path string, body io.Reader) (*http.Request, error) {
	addr, err := ipoc.sda.GetAddress(ctx)
	if err != nil {
		return nil, err
	}
	remoteURL := fmt.Sprintf("http://%s:%d%s", addr.Target, addr.Port, path)
	return http.NewRequest(method, remoteURL, body)
}

func (ipoc *ipOperatorClient) GetIPAddressStatus(ctx context.Context, orderID mtypes.OrderID) ([]ipoptypes.LeaseIPStatus, error) {
	path := fmt.Sprintf("/ip-lease-status/%s/%d/%d/%d", orderID.GetOwner(), orderID.GetDSeq(), orderID.GetGSeq(), orderID.GetOSeq())
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
		return nil, extractRemoteError(response)
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
	req, err := ipoc.newRequest(ctx, http.MethodGet, "/usage", nil)
	if err != nil {
		return ipoptypes.IPAddressUsage{}, err
	}

	response, err := ipoc.httpClient.Do(req)
	if err != nil {
		return ipoptypes.IPAddressUsage{}, err
	}
	ipoc.log.Info("usage result", "status", response.StatusCode)
	if response.StatusCode != http.StatusOK {
		return ipoptypes.IPAddressUsage{}, extractRemoteError(response)
	}

	decoder := json.NewDecoder(response.Body)
	result := ipoptypes.IPAddressUsage{}
	err = decoder.Decode(&result)
	if err != nil {
		return ipoptypes.IPAddressUsage{}, err
	}

	return result, nil
}

func ipOperatorClientRetry(ctx context.Context) []retry.Option {
	return []retry.Option{
		retry.Context(ctx),
		retry.DelayType(retry.BackOffDelay),
		retry.MaxDelay(time.Second * 120), // TODO - lower me
		retry.Attempts(15),
		retry.LastErrorOnly(true),
	}
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
	var response *http.Response
	err = retry.Do(func () error {
		var err error
		response, err = ipoc.httpClient.Do(req)
		if err != nil {
			ipoc.log.Error("http request failed", "err", err)
			ipoc.sda.DiscoverNow()
			return err
		}
		ipoc.log.Info("ip reservation request result", "status", response.StatusCode)

		if response.StatusCode != http.StatusOK {
			return extractRemoteError(response)
		}
		return nil
	}, ipOperatorClientRetry(ctx)...)

	if err != nil {
		return false, err
	}

	reserved := false
	dec := json.NewDecoder(response.Body)
	err = dec.Decode(&reserved)
	if err != nil {
		ipoc.log.Error("could not JSON decode response from IP operator for reservation", "err", err)
		return false, err
	}

	return reserved, nil
}

func extractRemoteError(response *http.Response) error{
	body := ipoptypes.IPOperatorErrorResponse{}
	decoder := json.NewDecoder(response.Body)
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

	return fmt.Errorf("status %d - %s", response.StatusCode, body.Error)
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
	err = retry.Do(func () error {
		response, err := ipoc.httpClient.Do(req)
		if err != nil {
			ipoc.log.Error("http request failed", "err", err)
			ipoc.sda.DiscoverNow()
			return err
		}
		if response.StatusCode != http.StatusNoContent {
			return extractRemoteError(response)
		}
		return nil
	}, ipOperatorClientRetry(ctx)...)
	return err
}
