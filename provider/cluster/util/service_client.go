package util

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"context"
	"io"
)

func (hwsc *httpWrapperServiceClient) CreateRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	serviceURL := fmt.Sprintf("%s/%s", hwsc.url, path)
	req, err := http.NewRequestWithContext(ctx, method, serviceURL, body)
	if err != nil {
		return nil, err
	}

	for k, v := range hwsc.headers {
		req.Header.Set(k ,v)
	}

	return req, nil
}

func (hwsc *httpWrapperServiceClient) DoRequest(req *http.Request) (*http.Response, error) {
	return hwsc.httpClient.Do(req)
}

func newHttpWrapperServiceClient(isHttps, secure bool, baseURL string) *httpWrapperServiceClient {
	netDialer := &net.Dialer{
		Timeout:       serviceClientTimeout,
	}

	// By default, block both things
	netDial := func(_ context.Context, network, addr string) (net.Conn, error) {
		return nil, fmt.Errorf("%w: cannot connect to %v:%v TLS must be used", errServiceClient, network, addr)
	}
	dialTLS := func(_ context.Context, network string, addr string) (net.Conn, error){
		return nil, fmt.Errorf("%w: cannot connect to %v:%v TLS is not supported", errServiceClient, network, addr)
	}

	// Unblock one of the dial methods
	if isHttps {
		tlsDialer := tls.Dialer{
			NetDialer: netDialer,
			Config: &tls.Config{
				InsecureSkipVerify: secure, // nolint:gosec
			},
		}
		dialTLS = tlsDialer.DialContext
	} else {
		netDial = netDialer.DialContext
	}

	httpClient := http.Client{
		Transport: &http.Transport{
			DialContext: netDial,
			DialTLSContext:         dialTLS,
			TLSHandshakeTimeout:    serviceClientTimeout,
			MaxIdleConns:           2,
			MaxConnsPerHost:        2,
			ResponseHeaderTimeout:  serviceClientTimeout,
			ExpectContinueTimeout:  serviceClientTimeout,
		},
		Timeout:      serviceClientTimeout * 2,
	}

	return &httpWrapperServiceClient{
		url: baseURL,
		httpClient: &httpClient,
	}

}
