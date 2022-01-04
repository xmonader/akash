package cmd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/ovrclk/akash/provider/cluster"
	clusterClient "github.com/ovrclk/akash/provider/cluster/kube"
	"github.com/ovrclk/akash/provider/cluster/kube/metallb"
	ctypes "github.com/ovrclk/akash/provider/cluster/types"
	clusterutil "github.com/ovrclk/akash/provider/cluster/util"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tendermint/tendermint/libs/log"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"net/http"
	"strconv"
	"sync"

	"time"

	ipoptypes "github.com/ovrclk/akash/provider/operator/ip_operator/types"
)

type managedIp struct {
	presentLease mtypes.LeaseID
	presentServiceName string
	lastEvent ctypes.IPResourceEvent
	presentSharingKey string
	presentExternalPort uint32
	presentPort uint32
	lastChangedAt time.Time
}

type ipReservationEntry struct {
	OrderID mtypes.OrderID
	Quantity uint
	QuantityAllocated uint
}

type ipOperator struct {
	state map[string]managedIp
	client cluster.Client
	log log.Logger
	server *operatorHttp
	leasesIgnored *ignoreList
	flagState prepareFlagFn
	flagIgnoredLeases prepareFlagFn

	available uint
	inUse uint

	kube kubernetes.Interface
	mllbc metallb.Client

	// TODO - is thig going to need to be persisted somehow?
	// The provider would basically be 'confused' if this operator restarted
	// and lost this data. Can we figure this out by just looking at the bids
	// currently open on the network by this provider ?
	reservations map[string]ipReservationEntry
	reservationsLock sync.Locker

	providerSda clusterutil.ServiceDiscoveryAgent
}

func (op *ipOperator) monitorUntilError(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	op.log.Info("starting observation")

	op.state = make(map[string]managedIp)

	entries, err := op.client.GetIPPassthroughs(ctx)
	if err != nil {
		cancel()
		return err
	}
	startupTime := time.Now()
	for _, ipPassThrough := range entries {
		k := getStateKey(ipPassThrough.GetLeaseID(), ipPassThrough.GetSharingKey(), ipPassThrough.GetExternalPort())
		op.state[k] = managedIp{
			presentLease:       ipPassThrough.GetLeaseID(),
			presentServiceName: ipPassThrough.GetServiceName(),
			lastEvent:          nil,
			presentSharingKey: ipPassThrough.GetSharingKey(),
			presentExternalPort: ipPassThrough.GetExternalPort(),
			presentPort: ipPassThrough.GetPort(),
			lastChangedAt:  startupTime,
		}
	}
	op.flagState()

	events, err := op.client.ObserveIPState(ctx)
	if err != nil {
		cancel()
		return err
	}

	var exitError error

	pruneTicker := time.NewTicker(2 * time.Minute /*op.cfg.pruneInterval*/)
	defer pruneTicker.Stop()
	prepareTicker := time.NewTicker(2 * time.Second /*op.cfg.webRefreshInterval*/)
	defer prepareTicker.Stop()

	const updateCountDelay = time.Second * 5
	isUpdating := true
	updateCountsTicker := time.NewTicker(updateCountDelay) // TODO - can we make this delay lower ?
	defer updateCountsTicker.Stop()
loop:
	for {
		eventsCopy := events
		if isUpdating {
			eventsCopy = nil
		}
		prepareData := false
		select {
		case <-ctx.Done():
			exitError = ctx.Err()
			break loop

		case ev, ok := <-eventsCopy:
			if !ok {
				exitError = errObservationStopped
				break loop
			}
			err = op.applyEvent(ctx, ev)
			if err != nil {
				op.log.Error("failed applying event", "err", err)
				exitError = err
				break loop
			}

			updateCountsTicker.Reset(updateCountDelay)
			isUpdating = true
		case <-pruneTicker.C:
			op.leasesIgnored.prune()
			op.flagIgnoredLeases()
			prepareData = true
		case <-prepareTicker.C:
			prepareData = true

		case <-updateCountsTicker.C:
			updateCountsTicker.Stop()
			err = op.updateCounts()
			if err != nil {
				exitError = err
				break loop
			}
			isUpdating = false
			prepareData = true
		}

		if prepareData {
			if err := op.server.prepareAll(); err != nil {
				op.log.Error("preparing web data failed", "err", err)
			}
		}
	}

	cancel()
	op.log.Info("ip operator done")
	return exitError
}

func (op *ipOperator) updateCounts() error {
	inUse, available, err := op.mllbc.GetIPAddressUsage()
	if err != nil {
		return err
	}

	op.inUse = inUse
	op.available = available
	op.log.Info("ip address inventory", "in-use", op.inUse, "available", op.available)
	return nil
}

func (op *ipOperator) recordEventError(ev ctypes.IPResourceEvent, failure error) {
	// ff no error, no action
	if failure == nil {
		return
	}

	mark := errorIsKubernetesResourceNotFound(failure)

	if !mark {
		return
	}

	op.log.Info("recording error for", "lease", ev.GetLeaseID().String(), "err", failure)
	op.leasesIgnored.addError(ev.GetLeaseID(), failure, ev.GetSharingKey())
	op.flagIgnoredLeases()
}

func (op *ipOperator) applyEvent(ctx context.Context, ev ctypes.IPResourceEvent) error {
	op.log.Debug("apply event", "event-type", ev.GetEventType(), "lease", ev.GetLeaseID())
	switch ev.GetEventType() {
	case ctypes.ProviderResourceDelete:
		// note that on delete the resource might be gone anyways because the namespace is deleted
		return op.applyDeleteEvent(ctx, ev)
	case ctypes.ProviderResourceAdd, ctypes.ProviderResourceUpdate:
		if op.leasesIgnored.isFlagged(ev.GetLeaseID()) {
			op.log.Info("ignoring event for", "lease", ev.GetLeaseID().String())
			return nil
		}
		err := op.applyAddOrUpdateEvent(ctx, ev)
		op.recordEventError(ev, err)
		return err
	default:
		return fmt.Errorf("%w: unknown event type %v", errObservationStopped, ev.GetEventType())
	}

}

func (op *ipOperator) applyDeleteEvent(ctx context.Context, ev ctypes.IPResourceEvent) error {
	directive := buildIPDirective(ev)
	err := op.client.PurgeIPPassthrough(ctx, ev.GetLeaseID(), directive)

	if err == nil {
		uid := getStateKeyFromEvent(ev)
		delete(op.state, uid)
		op.flagState()
	}

	return err
}

func buildIPDirective(ev ctypes.IPResourceEvent) ctypes.ClusterIPPassthroughDirective {
	return ctypes.ClusterIPPassthroughDirective{
		LeaseID:     ev.GetLeaseID(),
		ServiceName: ev.GetServiceName(),
		Port: ev.GetPort(),
		ExternalPort: ev.GetExternalPort(),
		SharingKey:  ev.GetSharingKey(),
		Protocol:  ev.GetProtocol(),
	}
}

func getStateKey(leaseID mtypes.LeaseID, sharingKey string, externalPort uint32) string {
	return fmt.Sprintf("%v-%s-%d", leaseID, sharingKey, externalPort)
}

func getStateKeyFromEvent(ev ctypes.IPResourceEvent) string{
	return getStateKey(ev.GetLeaseID(), ev.GetSharingKey(), ev.GetExternalPort())
}

func (op *ipOperator) decrInUse() uint {
	if op.inUse != 0 {
		op.inUse--
	}
	return op.inUse
}

func (op *ipOperator) incrInUse() uint {
	op.inUse++
	return op.inUse
}

func (op *ipOperator) applyAddOrUpdateEvent(ctx context.Context, ev ctypes.IPResourceEvent) error {
	leaseID := ev.GetLeaseID()

	uid := getStateKeyFromEvent(ev)

	op.log.Debug("connecting",
		"lease", leaseID,
		"service", ev.GetServiceName(),
		"externalPort", ev.GetExternalPort())
	entry, exists := op.state[uid]

	directive := buildIPDirective(ev)

	var err error
	shouldConnect := false

	if !exists {
		shouldConnect = true
		op.log.Debug("ip passthrough is new, applying")
		// Check to see if port or service name is different
	} else {
		hasChanged := entry.presentServiceName != ev.GetServiceName() ||
			entry.presentPort != ev.GetPort() ||
			entry.presentSharingKey != ev.GetSharingKey() ||
			entry.presentExternalPort != ev.GetExternalPort()
		if hasChanged {
			shouldConnect = true
			op.log.Debug("ip passthrough has changed, applying")
		}
	}

	if shouldConnect {
		op.log.Debug("Updating ip passthrough")
		err = op.client.CreateIPPassthrough(ctx, leaseID, directive)
	}

	if err == nil { // Update stored entry if everything went OK
		entry.presentServiceName = ev.GetServiceName()
		entry.presentLease = leaseID
		entry.lastEvent = ev
		entry.presentExternalPort = ev.GetExternalPort()
		entry.presentSharingKey = ev.GetSharingKey()
		entry.presentPort = ev.GetPort()
		entry.lastChangedAt = time.Now()
		op.state[uid] = entry
		op.flagState()

		reservationEntry := op.reservations[leaseID.String()]
		if 0 == reservationEntry.Quantity {
			op.log.Info("no reservation for IP", "leaseID", leaseID)
		} else {
			reservationEntry.QuantityAllocated++
			// TODO - bounds check this to make sure we don't somehow go over ?!
			op.reservations[leaseID.String()] = reservationEntry
		}
	}

	return err
}

func (op *ipOperator) webRouter() http.Handler {
	return op.server.router
}


func (op *ipOperator) prepareIgnoredLeases(pd *preparedResult) error {
	op.log.Debug("preparing ignore-list")
	return op.leasesIgnored.prepare(pd)
}

func (op *ipOperator) prepareState(pd *preparedResult) error {

	results := make(map[string][]interface{})
	for _, managedIpEntry := range op.state {
		leaseID := managedIpEntry.presentLease


		// TODO - add the resource name in kubernetes, for diagnostic reasons
		result := struct{
			LastChangeTime string `json:"last-event-time,omitempty"`
			LeaseID      mtypes.LeaseID `json:"lease-id"`
			Namespace    string `json:"namespace"` // diagnostic only
			Port uint32 `json:"port"`
			ExternalPort uint32 `json:"external-port"`
			ServiceName  string `json:"service-name"`
			SharingKey string `json:"sharing-key"`
		}{
			LeaseID:      leaseID,
			Namespace:    clusterutil.LeaseIDToNamespace(leaseID),
			Port:         managedIpEntry.presentPort,
			ExternalPort: managedIpEntry.presentExternalPort,
			ServiceName:  managedIpEntry.presentServiceName,
			SharingKey: managedIpEntry.presentSharingKey,
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

	pd.set(buf.Bytes())
	return nil
}


func handleReservationPost(op *ipOperator, rw http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var reservationRequest ipoptypes.IPReservationRequest
	err := decoder.Decode(&reservationRequest)
	if err != nil {
		handleHttpError(op, rw, req, err, http.StatusBadRequest)
		return
	}

	if reservationRequest.Quantity == 0 {
		handleHttpError(op, rw, req, ipoptypes.ErrReservationQuantityCannotBeZero, http.StatusBadRequest)
		return
	}

	reserved, err := op.addReservation(reservationRequest.OrderID, reservationRequest.Quantity)
	if err != nil {
		op.log.Error("could not make reservation", "leaseID", reservationRequest.OrderID, "quantity", reservationRequest.Quantity, "error", err)
		handleHttpError(op, rw, req, err, http.StatusInternalServerError)
		return
	}

	op.log.Info("added reservation", "order-id", reservationRequest.OrderID)
	rw.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(rw)
	err = enc.Encode(reserved)
	if err != nil {
		op.log.Error("could not write reservation response", "error", err)
	}
}

func (op *ipOperator) getReservationsCopy() (map[string]interface{}) {
	// This method exists as a way to deep copy the data while holding the lock
	// without blocking on anything external
	op.reservationsLock.Lock()
	defer op.reservationsLock.Unlock()
	result := make(map[string]interface{})

	for _, entry := range op.reservations {
		result[entry.OrderID.String()] = struct {
			OrderID mtypes.OrderID `json:"order-id"`
			Quantity uint `json:"quantity"`
			QuantityAllocated uint `json:"quantity-allocated"`
		}{
			OrderID: entry.OrderID,
			Quantity: entry.Quantity,
			QuantityAllocated: entry.QuantityAllocated,
		}
	}
	return result
}

func handleReservationGet(op *ipOperator, rw http.ResponseWriter, _ *http.Request) {
	rw.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(rw)
	err := encoder.Encode(op.getReservationsCopy())

	if err != nil {
		op.log.Error("could not write reservations response", "error", err)
		// Already wrote the header, so just bail
		return
	}
}


func handleHttpError(op *ipOperator, rw http.ResponseWriter, req *http.Request, err error, status int){
	op.log.Error("http request processing failed", "method", req.Method, "path", req.URL.Path, "err", err)
	rw.WriteHeader(status)

	body := ipoptypes.IPOperatorErrorResponse{
		Error: err.Error(),
		Code: -1,
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

func handleReservationDelete(op *ipOperator, rw http.ResponseWriter, req *http.Request){
	decoder := json.NewDecoder(req.Body)
	var deleteRequest ipoptypes.IPReservationDelete
	err := decoder.Decode(&deleteRequest)

	if err != nil {
		handleHttpError(op, rw, req, err, http.StatusBadRequest)
		return
	}

	err = op.removeReservation(deleteRequest.OrderID)
	if err == nil {
		op.log.Info("removed reservation", "order-id", deleteRequest.OrderID)
		rw.WriteHeader(http.StatusNoContent)
		return
	}

	if errors.Is(err, ipoptypes.ErrNoSuchReservation) {
		handleHttpError(op, rw, req, err, http.StatusBadRequest)
		return
	}

	handleHttpError(op, rw, req, err, http.StatusInternalServerError)
	return
}

func (op *ipOperator) removeReservation(orderID mtypes.OrderID) error {
	op.reservationsLock.Lock()
	defer op.reservationsLock.Unlock()

	_, exists := op.reservations[orderID.String()]
	if !exists {
		return ipoptypes.ErrNoSuchReservation
	}

	delete(op.reservations, orderID.String())
	return nil
}

func newIpOperator(logger log.Logger, client cluster.Client, ilc ignoreListConfig, mllbc metallb.Client, providerSda clusterutil.ServiceDiscoveryAgent) (*ipOperator) {
	retval := &ipOperator{
		state:  make(map[string]managedIp),
		client: client,
		log:    logger,
		server: newOperatorHttp(),
		leasesIgnored: newIgnoreList(ilc),
		mllbc: mllbc,
		reservations: make(map[string]ipReservationEntry),
		reservationsLock: &sync.Mutex{},
		providerSda: providerSda,
	}

	retval.flagState = retval.server.addPreparedEndpoint("/state", retval.prepareState)
	retval.flagIgnoredLeases = retval.server.addPreparedEndpoint("/ignored-leases", retval.leasesIgnored.prepare)

	// TODO - add auth based off TokenReview via k8s interface to below methods
	retval.server.router.HandleFunc("/reservations", func(rw http.ResponseWriter, req *http.Request){
		clientID := req.Header.Get("X-Client-Id")
		_ = clientID

		if req.Method == http.MethodPost {
			handleReservationPost(retval, rw, req)
		} else if req.Method == http.MethodDelete {
			handleReservationDelete(retval, rw, req)
		} else {
			handleReservationGet(retval, rw, req)
		}


	}).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)


	retval.server.router.HandleFunc("/ip-lease-status/{owner}/{dseq}/{gseq}/{oseq}", func(rw http.ResponseWriter, req *http.Request){
		handleIPLeaseStatusGet(retval, rw, req)
	}).Methods(http.MethodGet)
	return retval
}

func handleIPLeaseStatusGet(op *ipOperator, rw http.ResponseWriter, req *http.Request){

	addr, err := op.providerSda.GetAddress(req.Context())
	if err != nil {
		handleHttpError(op, rw, req, err, http.StatusInternalServerError)
		return
	}

	// TODO - move this fetch code at least to a utility function or something
	tlsDialer := tls.Dialer{
		NetDialer: nil,
		Config:    &tls.Config{
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
		Transport:     &http.Transport{
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
		Timeout:       0,
	}

	statusUrl := fmt.Sprintf("https://%s:%d/status", addr.Target, addr.Port)
	statusReq, err := http.NewRequestWithContext(req.Context(), http.MethodGet, statusUrl, nil)
	if err != nil {
		handleHttpError(op, rw, req, err, http.StatusInternalServerError)
		return
	}
	response, err := httpClient.Do(statusReq)
	if err != nil {
		handleHttpError(op, rw, req, err, http.StatusInternalServerError)
		return
	}

	if response.StatusCode != http.StatusOK {
		op.log.Error("provider status API failed", "status", response.StatusCode)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}


	statusData := struct{
		Address string `json:"address"`
	}{}
	decoder := json.NewDecoder(response.Body)
	err = decoder.Decode(&statusData)
	if err != nil {
		handleHttpError(op, rw, req, err, http.StatusInternalServerError)
		return
	}
	providerAddr := statusData.Address
	if len(providerAddr) == 0 {
		op.log.Error("provider status API did not include address information")
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}


	vars := mux.Vars(req)

	dseqStr := vars["dseq"]
	dseq, err := strconv.ParseUint(dseqStr, 10, 64)
	if err != nil {
		op.log.Info("could not parse path component as uint64","dseq", dseqStr, "error", err)
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	gseqStr := vars["gseq"]
	gseq, err := strconv.ParseUint(gseqStr, 10, 32)
	if err != nil {
		op.log.Info("could not parse path component as uint32","gseq", gseqStr, "error", err)
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	oseqStr := vars["oseq"]
	oseq, err := strconv.ParseUint(oseqStr, 10, 32)
	if err != nil {
		op.log.Info("could not parse path component as uint32","oseq", oseqStr, "error", err)
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	owner := vars["owner"] // TODO  - validate this is a bech32 addr


	leaseID := mtypes.LeaseID{
		Owner:    owner,
		DSeq:     dseq,
		GSeq:     uint32(gseq),
		OSeq:     uint32(oseq),
		Provider: providerAddr,
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
	// ipStaus is a slice of interface types, so it can't be encoded directly
	responseData := make([]ipoptypes.LeaseIPStatus, len(ipStatus))
	for i, v := range ipStatus {
		responseData[i] = ipoptypes.LeaseIPStatus{
			Port: v.GetPort(),
			ExternalPort: v.GetExternalPort(),
			ServiceName: v.GetServiceName(),
			IP: v.GetIP(),
			Protocol: v.GetProtocol().ToString(),
		}
	}
	err = encoder.Encode(responseData)
	if err != nil {
		op.log.Error("failed writing JSON of ip status response", "error", err)
	}
}

func (op *ipOperator) addReservation(orderID mtypes.OrderID, quantity uint) (bool, error) {
	op.reservationsLock.Lock()
	defer op.reservationsLock.Unlock()

	qtyReserved := uint(0)
	available := op.available
	for _, entry := range op.reservations {
		// Add the amount normally reserved
		qtyReserved += entry.Quantity
		// But take out the amount actually allocated
		qtyReserved -= entry.QuantityAllocated

		if entry.OrderID == orderID {
			available += entry.Quantity
		}
	}

	// TODO - refresh inUse beforehand  ?
	if quantity > (available - op.inUse - qtyReserved) {
		return false, nil
	}

	op.reservations[orderID.String()] = ipReservationEntry{
		OrderID:  orderID,
		Quantity: quantity,
	}

	return true, nil
}

func doIPOperator(cmd *cobra.Command) error {
	ns := viper.GetString(FlagK8sManifestNS)
	listenAddr := viper.GetString(FlagListenAddress)
	logger := openLogger().With("operator","ip")

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

	logger.Info("clients","kube", client, "metallb", mllbc)

	op := newIpOperator(logger, client, ignoreListConfigFromViper(), mllbc, providerSda)
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
	addOperatorFlags(cmd,"0.0.0.0:8086")

	return cmd
}

func (op *ipOperator) run(parentCtx context.Context) error {
	op.log.Debug("ip operator start")
	const threshold = 3 * time.Second
	for {
		lastAttempt := time.Now()
		err := op.monitorUntilError(parentCtx)
		if errors.Is(err, context.Canceled) {
			op.log.Debug("ip operator terminate")
			return err
		}

		op.log.Error("observation stopped", "err", err)

		// don't spin if there is a condition causing fast failure
		elapsed := time.Since(lastAttempt)
		if elapsed < threshold {
			op.log.Info("delaying")
			select {
			case <-parentCtx.Done():
				return parentCtx.Err()
			case <-time.After(threshold):
				// delay complete
			}
		}
	}
}
