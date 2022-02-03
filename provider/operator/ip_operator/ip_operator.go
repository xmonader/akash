package ip_operator

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/avast/retry-go"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/mux"
	"github.com/ovrclk/akash/provider/cluster"
	clusterClient "github.com/ovrclk/akash/provider/cluster/kube"
	"github.com/ovrclk/akash/provider/cluster/kube/metallb"
	"github.com/ovrclk/akash/provider/cluster/types/v1beta2"
	ctypes "github.com/ovrclk/akash/provider/cluster/types/v1beta2"
	clusterutil "github.com/ovrclk/akash/provider/cluster/util"
	provider_flags "github.com/ovrclk/akash/provider/cmd/flags"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tendermint/tendermint/libs/log"
	"golang.org/x/sync/errgroup"
	"io"
	kubeErrors "k8s.io/apimachinery/pkg/api/errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ipoptypes "github.com/ovrclk/akash/provider/operator/ip_operator/types"
	"github.com/ovrclk/akash/provider/operator/operator_common"
)

var (
	errIPOperator = errors.New("ip operator failure")
)

type managedIp struct {
	presentLease        mtypes.LeaseID
	presentServiceName  string
	lastEvent           v1beta2.IPResourceEvent
	presentSharingKey   string
	presentExternalPort uint32
	presentPort         uint32
	lastChangedAt       time.Time
}

type ipOperator struct {
	state             map[string]managedIp
	client            cluster.Client
	log               log.Logger
	server            operator_common.OperatorHttp
	leasesIgnored     operator_common.IgnoreList
	flagState         operator_common.PrepareFlagFn
	flagIgnoredLeases operator_common.PrepareFlagFn
	flagUsage         operator_common.PrepareFlagFn
	providerAddr      string

	available uint
	inUse     uint

	mllbc metallb.Client

	providerSda clusterutil.ServiceDiscoveryAgent
	barrier     *barrier

	dataLock sync.Locker
}

func (op *ipOperator) monitorUntilError(parentCtx context.Context) error {
	var err error

	op.log.Info("getting provider address")

	op.providerAddr, err = op.getProviderWalletAddress(parentCtx)
	if err != nil {
		return err
	}
	op.log.Info("associated provider ", "addr", op.providerAddr)

	op.state = make(map[string]managedIp)
	op.log.Info("fetching existing IP passthroughs")
	entries, err := op.mllbc.GetIPPassthroughs(parentCtx)
	if err != nil {
		return err
	}
	startupTime := time.Now()
	for _, ipPassThrough := range entries {
		k := getStateKey(ipPassThrough.GetLeaseID(), ipPassThrough.GetSharingKey(), ipPassThrough.GetExternalPort())
		op.state[k] = managedIp{
			presentLease:        ipPassThrough.GetLeaseID(),
			presentServiceName:  ipPassThrough.GetServiceName(),
			lastEvent:           nil,
			presentSharingKey:   ipPassThrough.GetSharingKey(),
			presentExternalPort: ipPassThrough.GetExternalPort(),
			presentPort:         ipPassThrough.GetPort(),
			lastChangedAt:       startupTime,
		}
	}
	op.flagState()

	// Get the present counts before starting
	err = op.updateCounts(parentCtx)
	if err != nil {
		return err
	}

	op.log.Info("starting observation")

	events, err := op.client.ObserveIPState(parentCtx)
	if err != nil {
		return err
	}

	if err := op.server.PrepareAll(); err != nil {
		return err
	}

	var exitError error

	pruneTicker := time.NewTicker(2 * time.Minute /*op.cfg.pruneInterval*/)
	defer pruneTicker.Stop()
	prepareTicker := time.NewTicker(2 * time.Second /*op.cfg.webRefreshInterval*/)
	defer prepareTicker.Stop()

	op.log.Info("barrier can now be passed")
	op.barrier.enable()
loop:
	for {
		isUpdating := false
		prepareData := false
		select {
		case <-parentCtx.Done():
			exitError = parentCtx.Err()
			break loop

		case ev, ok := <-events:
			if !ok {
				exitError = operator_common.ErrObservationStopped
				break loop
			}
			err = op.applyEvent(parentCtx, ev)
			if err != nil {
				op.log.Error("failed applying event", "err", err)
				exitError = err
				break loop
			}

			isUpdating = true
		case <-pruneTicker.C:
			op.leasesIgnored.Prune()
			op.flagIgnoredLeases()
			prepareData = true
		case <-prepareTicker.C:
			prepareData = true
		}

		if isUpdating {
			err = op.updateCounts(parentCtx)
			if err != nil {
				exitError = err
				break loop
			}
			isUpdating = false
			prepareData = true
		}

		if prepareData {
			if err := op.server.PrepareAll(); err != nil {
				op.log.Error("preparing web data failed", "err", err)
			}
		}
	}
	op.barrier.disable()

	// Wait for up to 30 seconds
	ctxWithTimeout, timeoutCtxCancel := context.WithTimeout(context.Background(), time.Second*30)
	defer timeoutCtxCancel()

	err = op.barrier.waitUntilClear(ctxWithTimeout)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			op.log.Error("did not clear barrier in given time")
		} else {
			op.log.Error("failed waiting on barrier to clear", "err", err)
		}
	}

	op.log.Info("ip operator done")

	return exitError
}

func (op *ipOperator) updateCounts(ctx context.Context) error {
	inUse, available, err := op.mllbc.GetIPAddressUsage(ctx)
	if err != nil {
		return err
	}

	op.dataLock.Lock()
	defer op.dataLock.Unlock()
	op.inUse = inUse
	op.available = available

	op.flagUsage()
	op.log.Info("ip address inventory", "in-use", op.inUse, "available", op.available)
	return nil
}

func (op *ipOperator) recordEventError(ev v1beta2.IPResourceEvent, failure error) {
	// ff no error, no action
	if failure == nil {
		return
	}

	mark := kubeErrors.IsNotFound(failure)

	if !mark {
		return
	}

	op.log.Info("recording error for", "lease", ev.GetLeaseID().String(), "err", failure)
	op.leasesIgnored.AddError(ev.GetLeaseID(), failure, ev.GetSharingKey())
	op.flagIgnoredLeases()
}

func (op *ipOperator) applyEvent(ctx context.Context, ev v1beta2.IPResourceEvent) error {
	op.log.Debug("apply event", "event-type", ev.GetEventType(), "lease", ev.GetLeaseID())
	switch ev.GetEventType() {
	case ctypes.ProviderResourceDelete:
		// note that on delete the resource might be gone anyways because the namespace is deleted
		return op.applyDeleteEvent(ctx, ev)
	case ctypes.ProviderResourceAdd, ctypes.ProviderResourceUpdate:
		if op.leasesIgnored.IsFlagged(ev.GetLeaseID()) {
			op.log.Info("ignoring event for", "lease", ev.GetLeaseID().String())
			return nil
		}
		err := op.applyAddOrUpdateEvent(ctx, ev)
		op.recordEventError(ev, err)
		return err
	default:
		return fmt.Errorf("%w: unknown event type %v", operator_common.ErrObservationStopped, ev.GetEventType())
	}
}

func (op *ipOperator) applyDeleteEvent(ctx context.Context, ev v1beta2.IPResourceEvent) error {
	directive := buildIPDirective(ev)
	err := op.mllbc.PurgeIPPassthrough(ctx, ev.GetLeaseID(), directive)

	if err == nil {
		uid := getStateKeyFromEvent(ev)
		delete(op.state, uid)
		op.flagState()
	}

	return err
}

func buildIPDirective(ev v1beta2.IPResourceEvent) ctypes.ClusterIPPassthroughDirective {
	return ctypes.ClusterIPPassthroughDirective{
		LeaseID:      ev.GetLeaseID(),
		ServiceName:  ev.GetServiceName(),
		Port:         ev.GetPort(),
		ExternalPort: ev.GetExternalPort(),
		SharingKey:   ev.GetSharingKey(),
		Protocol:     ev.GetProtocol(),
	}
}

func getStateKey(leaseID mtypes.LeaseID, sharingKey string, externalPort uint32) string {
	return fmt.Sprintf("%v-%s-%d", leaseID, sharingKey, externalPort)
}

func getStateKeyFromEvent(ev v1beta2.IPResourceEvent) string {
	return getStateKey(ev.GetLeaseID(), ev.GetSharingKey(), ev.GetExternalPort())
}

func (op *ipOperator) applyAddOrUpdateEvent(ctx context.Context, ev v1beta2.IPResourceEvent) error {
	leaseID := ev.GetLeaseID()

	uid := getStateKeyFromEvent(ev)

	op.log.Info("connecting",
		"lease", leaseID,
		"service", ev.GetServiceName(),
		"externalPort", ev.GetExternalPort())
	entry, exists := op.state[uid]

	directive := buildIPDirective(ev)

	var err error
	shouldConnect := false

	if !exists {
		shouldConnect = true
		op.log.Debug("ip passthrough is new, applying", "lease", leaseID)
		// Check to see if port or service name is different
	} else {
		hasChanged := entry.presentServiceName != ev.GetServiceName() ||
			entry.presentPort != ev.GetPort() ||
			entry.presentSharingKey != ev.GetSharingKey() ||
			entry.presentExternalPort != ev.GetExternalPort()
		if hasChanged {
			shouldConnect = true
			op.log.Debug("ip passthrough has changed, applying", "lease", leaseID)
		}
	}

	if shouldConnect {
		op.log.Debug("Updating ip passthrough", "lease", leaseID)
		err = op.mllbc.CreateIPPassthrough(ctx, leaseID, directive)
	}

	if err != nil {
		return err
	}

	// Update stored entry
	entry.presentServiceName = ev.GetServiceName()
	entry.presentLease = leaseID
	entry.lastEvent = ev
	entry.presentExternalPort = ev.GetExternalPort()
	entry.presentSharingKey = ev.GetSharingKey()
	entry.presentPort = ev.GetPort()
	entry.lastChangedAt = time.Now()
	op.state[uid] = entry
	op.flagState()

	op.log.Info("update complete", "lease", leaseID)

	return nil
}

func (op *ipOperator) webRouter() http.Handler {
	return op.server.GetRouter()
}

func (op *ipOperator) prepareUsage(pd operator_common.PreparedResult) error {
	op.dataLock.Lock()
	defer op.dataLock.Unlock()
	value := ipoptypes.IPAddressUsage{
		Available: op.available,
		InUse:     op.inUse,
	}

	buf := &bytes.Buffer{}
	encoder := json.NewEncoder(buf)

	err := encoder.Encode(value)
	if err != nil {
		return err
	}

	pd.Set(buf.Bytes())
	return nil
}

func (op *ipOperator) prepareState(pd operator_common.PreparedResult) error {
	results := make(map[string][]interface{})
	for _, managedIpEntry := range op.state {
		leaseID := managedIpEntry.presentLease

		result := struct {
			LastChangeTime string         `json:"last-event-time,omitempty"`
			LeaseID        mtypes.LeaseID `json:"lease-id"`
			Namespace      string         `json:"namespace"` // diagnostic only
			Port           uint32         `json:"port"`
			ExternalPort   uint32         `json:"external-port"`
			ServiceName    string         `json:"service-name"`
			SharingKey     string         `json:"sharing-key"`
		}{
			LeaseID:        leaseID,
			Namespace:      clusterutil.LeaseIDToNamespace(leaseID),
			Port:           managedIpEntry.presentPort,
			ExternalPort:   managedIpEntry.presentExternalPort,
			ServiceName:    managedIpEntry.presentServiceName,
			SharingKey:     managedIpEntry.presentSharingKey,
			LastChangeTime: managedIpEntry.lastChangedAt.UTC().String(),
		}

		entryList := results[leaseID.String()]
		entryList = append(entryList, result)
		results[leaseID.String()] = entryList
	}

	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err := enc.Encode(results)
	if err != nil {
		return err
	}

	pd.Set(buf.Bytes())
	return nil
}

func handleHttpError(op *ipOperator, rw http.ResponseWriter, req *http.Request, err error, status int) {
	op.log.Error("http request processing failed", "method", req.Method, "path", req.URL.Path, "err", err)
	rw.WriteHeader(status)

	body := ipoptypes.IPOperatorErrorResponse{
		Error: err.Error(),
		Code:  -1,
	}

	if errors.Is(err, ipoptypes.ErrIPOperator) {
		code := err.(ipoptypes.IPOperatorError).GetCode()
		body.Code = code
	}

	encoder := json.NewEncoder(rw)
	err = encoder.Encode(body)
	if err != nil {
		op.log.Error("failed writing response body", "err", err)
	}
}

func (op *ipOperator) getProviderWalletAddress(ctx context.Context) (string, error) {
	netDialer := &net.Dialer{
		Timeout:       10 * time.Second,
		Deadline:      time.Time{},
		LocalAddr:     nil,
		DualStack:     false,
		FallbackDelay: 0,
		KeepAlive:     0,
		Resolver:      nil,
		Cancel:        nil,
		Control:       nil,
	}
	tlsDialer := tls.Dialer{
		NetDialer: netDialer,
		Config: &tls.Config{
			Rand:                        nil,
			Time:                        nil,
			Certificates:                nil,
			NameToCertificate:           nil,
			GetCertificate:              nil,
			GetClientCertificate:        nil,
			GetConfigForClient:          nil,
			VerifyPeerCertificate:       nil,
			VerifyConnection:            nil,
			RootCAs:                     nil,
			NextProtos:                  nil,
			ServerName:                  "",
			ClientAuth:                  0,
			ClientCAs:                   nil,
			InsecureSkipVerify:          true,
			CipherSuites:                nil,
			PreferServerCipherSuites:    false,
			SessionTicketsDisabled:      false,
			SessionTicketKey:            [32]byte{},
			ClientSessionCache:          nil,
			MinVersion:                  0,
			MaxVersion:                  0,
			CurvePreferences:            nil,
			DynamicRecordSizingDisabled: false,
			Renegotiation:               0,
			KeyLogWriter:                nil,
		},
	}
	httpClient := http.Client{
		Transport: &http.Transport{
			Proxy:                  nil,
			DialContext:            nil,
			Dial:                   nil,
			DialTLSContext:         tlsDialer.DialContext,
			DialTLS:                nil,
			TLSClientConfig:        nil,
			TLSHandshakeTimeout:    0,
			DisableKeepAlives:      false,
			DisableCompression:     false,
			MaxIdleConns:           0,
			MaxIdleConnsPerHost:    0,
			MaxConnsPerHost:        0,
			IdleConnTimeout:        0,
			ResponseHeaderTimeout:  0,
			ExpectContinueTimeout:  0,
			TLSNextProto:           nil,
			ProxyConnectHeader:     nil,
			GetProxyConnectHeader:  nil,
			MaxResponseHeaderBytes: 0,
			WriteBufferSize:        0,
			ReadBufferSize:         0,
			ForceAttemptHTTP2:      false,
		},
		CheckRedirect: nil,
		Jar:           nil,
		Timeout:       30 * time.Second,
	}

	// Resolve the hostname & port
	addr, err := op.providerSda.GetAddress(ctx)
	if err != nil {
		op.log.Error("could not discover provider address", "error", err)
		return "", err
	}

	statusUrl := fmt.Sprintf("https://%s:%d/address", addr.Target, addr.Port)
	statusReq, err := http.NewRequestWithContext(ctx, http.MethodGet, statusUrl, nil)
	if err != nil {
		return "", err
	}

	retryOptions := []retry.Option{
		retry.Context(ctx),
		retry.DelayType(retry.BackOffDelay),
		retry.MaxDelay(time.Second * 15),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	}

	var response *http.Response
	err = retry.Do(func() error {
		var err error
		response, err = httpClient.Do(statusReq)
		if err != nil {
			op.log.Error("failed asking provider for status", "error", err)
			return err
		}
		return nil
	}, retryOptions...)

	if err != nil {
		return "", err
	}

	if response.StatusCode != http.StatusOK {
		op.log.Error("provider status API failed", "status", response.StatusCode)
		return "", fmt.Errorf("%w: provider status API returned status: %d", errIPOperator, response.StatusCode)
	}

	statusData := struct {
		Address string `json:"address"`
	}{}
	decoder := json.NewDecoder(response.Body)
	err = decoder.Decode(&statusData)
	if err != nil {
		op.log.Error("could not decode provider status API response", "error", err)
		return "", err

	}
	providerAddr := statusData.Address

	_, err = sdk.AccAddressFromBech32(providerAddr)
	if err != nil {
		op.log.Error("provider status API returned invalid bech32 address", "provider-addr", providerAddr, "error", err)
		return "", err
	}

	return providerAddr, nil
}

func newIpOperator(logger log.Logger, client cluster.Client, ilc operator_common.IgnoreListConfig, mllbc metallb.Client, providerSda clusterutil.ServiceDiscoveryAgent) (*ipOperator, error) {
	opHttp, err := operator_common.NewOperatorHttp()
	if err != nil {
		return nil, err
	}
	retval := &ipOperator{
		state:         make(map[string]managedIp),
		client:        client,
		log:           logger,
		server:        opHttp,
		leasesIgnored: operator_common.NewIgnoreList(ilc),
		mllbc:         mllbc,
		dataLock:      &sync.Mutex{},
		providerSda:   providerSda,
		barrier:       &barrier{},
	}

	retval.server.GetRouter().Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if !retval.barrier.enter() {
				retval.log.Error("barrier is locked, can't service request", "path", req.URL.Path)
				rw.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(rw, req)
			retval.barrier.exit()
		})
	})

	retval.flagState = retval.server.AddPreparedEndpoint("/state", retval.prepareState)
	retval.flagIgnoredLeases = retval.server.AddPreparedEndpoint("/ignored-leases", retval.leasesIgnored.Prepare)
	retval.flagUsage = retval.server.AddPreparedEndpoint("/usage", retval.prepareUsage)

	retval.server.GetRouter().HandleFunc("/health", func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(rw, "OK")
	})

	// TODO - add auth based off TokenReview via k8s interface to below methods OR just cache these so they can't abuse kube
	retval.server.GetRouter().HandleFunc("/ip-lease-status/{owner}/{dseq}/{gseq}/{oseq}", func(rw http.ResponseWriter, req *http.Request) {
		handleIPLeaseStatusGet(retval, rw, req)
	}).Methods(http.MethodGet)
	return retval, nil
}

func handleIPLeaseStatusGet(op *ipOperator, rw http.ResponseWriter, req *http.Request) {
	// Extract path variables, returning 404 if any are invalid
	vars := mux.Vars(req)
	dseqStr := vars["dseq"]
	dseq, err := strconv.ParseUint(dseqStr, 10, 64)
	if err != nil {
		op.log.Error("could not parse path component as uint64", "dseq", dseqStr, "error", err)
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	gseqStr := vars["gseq"]
	gseq, err := strconv.ParseUint(gseqStr, 10, 32)
	if err != nil {
		op.log.Error("could not parse path component as uint32", "gseq", gseqStr, "error", err)
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	oseqStr := vars["oseq"]
	oseq, err := strconv.ParseUint(oseqStr, 10, 32)
	if err != nil {
		op.log.Error("could not parse path component as uint32", "oseq", oseqStr, "error", err)
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	owner := vars["owner"]
	_, err = sdk.AccAddressFromBech32(owner) // Validate this is a bech32 address
	if err != nil {
		op.log.Error("could not parse owner address as bech32", "onwer", owner, "error", err)
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	leaseID := mtypes.LeaseID{
		Owner:    owner,
		DSeq:     dseq,
		GSeq:     uint32(gseq),
		OSeq:     uint32(oseq),
		Provider: op.providerAddr,
	}

	ipStatus, err := op.mllbc.GetIPAddressStatusForLease(req.Context(), leaseID)
	if err != nil {
		op.log.Error("Could not get IP address status", "lease-id", leaseID, "error", err)
		handleHttpError(op, rw, req, err, http.StatusInternalServerError)
		return
	}

	if len(ipStatus) == 0 {
		rw.WriteHeader(http.StatusNoContent)
		return
	}

	rw.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(rw)
	// ipStatus is a slice of interface types, so it can't be encoded directly
	responseData := make([]ipoptypes.LeaseIPStatus, len(ipStatus))
	for i, v := range ipStatus {
		responseData[i] = ipoptypes.LeaseIPStatus{
			Port:         v.GetPort(),
			ExternalPort: v.GetExternalPort(),
			ServiceName:  v.GetServiceName(),
			IP:           v.GetIP(),
			Protocol:     v.GetProtocol().ToString(),
		}
	}
	err = encoder.Encode(responseData)
	if err != nil {
		op.log.Error("failed writing JSON of ip status response", "error", err)
	}
}

func doIPOperator(cmd *cobra.Command) error {
	ns := viper.GetString(provider_flags.FlagK8sManifestNS)
	listenAddr := viper.GetString(provider_flags.FlagListenAddress)
	logger := operator_common.OpenLogger().With("operator", "ip")

	// Config path not provided because the authorization comes from the role assigned to the deployment
	// and provided by kubernetes
	configPath := ""
	client, err := clusterClient.NewClient(logger, ns, configPath)
	if err != nil {
		return err
	}

	mllbc, err := metallb.NewClient(configPath, logger)
	if err != nil {
		return err
	}

	providerSda := clusterutil.NewServiceDiscoveryAgent(logger, "gateway", "akash-provider", "akash-services", "TCP")
	logger.Info("clients", "kube", client, "metallb", mllbc)

	op, err := newIpOperator(logger, client, operator_common.IgnoreListConfigFromViper(), mllbc, providerSda)
	if err != nil {
		return err
	}
	router := op.webRouter()
	group, ctx := errgroup.WithContext(cmd.Context())

	group.Go(func() error {
		srv := http.Server{Addr: listenAddr, Handler: router}
		go func() {
			<-ctx.Done()
			_ = srv.Close()
		}()
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})

	group.Go(func() error {
		return op.run(ctx)
	})

	err = group.Wait()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func IPOperatorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "ip-operator",
		Short:        "kubernetes operator interfacing with Metal LB",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return doIPOperator(cmd)
		},
	}
	operator_common.AddOperatorFlags(cmd, "0.0.0.0:8086")
	operator_common.AddIgnoreListFlags(cmd)

	return cmd
}

func (op *ipOperator) run(parentCtx context.Context) error {
	op.log.Debug("ip operator start")
	const threshold = 500 * time.Millisecond
	for {
		lastAttempt := time.Now()
		err := op.monitorUntilError(parentCtx)
		if errors.Is(err, context.Canceled) {
			op.log.Debug("ip operator terminate")
			break
		}

		op.log.Error("observation stopped", "err", err)

		// don't spin if there is a condition causing fast failure
		elapsed := time.Since(lastAttempt)
		if elapsed < threshold {
			op.log.Info("delaying")
			select {
			case <-parentCtx.Done():
				break

			case <-time.After(threshold):
				// delay complete
			}
		}
	}

	op.providerSda.Stop()
	op.mllbc.Stop()
	return parentCtx.Err()
}

type barrier struct {
	enabled int32
	active  int32
}

func (b *barrier) enable() {
	atomic.StoreInt32(&b.enabled, 1)
}

func (b *barrier) disable() {
	atomic.StoreInt32(&b.enabled, 0)
}

func (b *barrier) enter() bool {
	isEnabled := atomic.LoadInt32(&b.enabled) == 1
	if !isEnabled {
		return false
	}

	atomic.AddInt32(&b.active, 1)
	return true
}

func (b *barrier) exit() {
	atomic.StoreInt32(&b.active, -1)
}

func (b *barrier) waitUntilClear(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			clear := 0 == atomic.LoadInt32(&b.active)
			if clear {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
