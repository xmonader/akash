package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ovrclk/akash/manifest"
	crd "github.com/ovrclk/akash/pkg/apis/akash.network/v1"
	"github.com/ovrclk/akash/provider/cluster"
	clusterClient "github.com/ovrclk/akash/provider/cluster/kube"
	ctypes "github.com/ovrclk/akash/provider/cluster/types"
	"github.com/ovrclk/akash/provider/cluster/util"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tendermint/tendermint/libs/log"
	"golang.org/x/sync/errgroup"
	kubeErrors "k8s.io/apimachinery/pkg/api/errors"
	"net/http"
	"strings"
	"time"

	clusterutil "github.com/ovrclk/akash/provider/cluster/util"
)

const (
	FlagListenAddress        = "listen"
	FlagPruneInterval        = "prune-interval"
	FlagIgnoreListEntryLimit = "ignore-list-entry-limit"
	FlagWebRefreshInterval   = "web-refresh-interval"
	FlagRetryDelay           = "retry-delay"
	FlagIgnoreListAgeLimit   = "ignore-list-age-limit"
	FlagEventFailureLimit    = "event=failure-limit"
)

var (
	errObservationStopped       = errors.New("observation stopped")
	errExpectedResourceNotFound = fmt.Errorf("%w: resource not found", errObservationStopped)
)



type hostnameOperator struct {
	hostnames  map[string]managedHostname
	//ignoreList map[mtypes.LeaseID]ignoreListEntry

	leasesIgnored *ignoreList

	client cluster.Client

	log log.Logger

	cfg hostnameOperatorConfig
	server *operatorHttp

	flagHostnamesData prepareFlagFn
	flagIgnoreListData prepareFlagFn
}

func (op *hostnameOperator) run(parentCtx context.Context) error {
	op.log.Debug("hostname operator start")

	for {
		lastAttempt := time.Now()
		err := op.monitorUntilError(parentCtx)
		if errors.Is(err, context.Canceled) {
			op.log.Debug("hostname operator terminate")
			return err
		}

		op.log.Error("observation stopped", "err", err)

		// don't spin if there is a condition causing fast failure
		elapsed := time.Since(lastAttempt)
		if elapsed < op.cfg.retryDelay {
			op.log.Info("delaying")
			select {
			case <-parentCtx.Done():
				return parentCtx.Err()
			case <-time.After(op.cfg.retryDelay):
				// delay complete
			}
		}
	}
}

func servePreparedResult(rw http.ResponseWriter, pd *preparedResult) {
	rw.Header().Set("Cache-Control", "no-cache, max-age=0")
	value := pd.get()
	if len(value.data) == 0 {
		rw.WriteHeader(http.StatusNoContent)
		return
	}

	rw.Header().Set("Last-Modified", value.preparedAt.UTC().Format(http.TimeFormat))
	rw.WriteHeader(http.StatusOK)
	_, _ = rw.Write(value.data)
}



func (op *hostnameOperator) webRouter() http.Handler {
	return op.server.router
}

func (op *hostnameOperator) monitorUntilError(parentCtx context.Context) error {
	/*
		Note - the only possible enhancement here would be to enumerate all
		Ingress objects in the kube cluster not managed by Akash & then
		avoid trying to create Ingress objects with those names. This isn't really
		needed at this time.
	*/
	op.hostnames = make(map[string]managedHostname)
	ctx, cancel := context.WithCancel(parentCtx)
	op.log.Info("starting observation")

	connections, err := op.client.GetHostnameDeploymentConnections(ctx)
	if err != nil {
		cancel()
		return err
	}

	for _, conn := range connections {
		leaseID := conn.GetLeaseID()
		hostname := conn.GetHostname()
		entry := managedHostname{
			lastEvent:           nil,
			presentLease:        leaseID,
			presentServiceName:  conn.GetServiceName(),
			presentExternalPort: uint32(conn.GetExternalPort()),
		}

		op.hostnames[hostname] = entry
		op.log.Debug("identified existing hostname connection",
			"hostname", hostname,
			"lease", entry.presentLease,
			"service", entry.presentServiceName,
			"port", entry.presentExternalPort)
	}
	op.flagHostnamesData()

	events, err := op.client.ObserveHostnameState(ctx)
	if err != nil {
		cancel()
		return err
	}

	pruneTicker := time.NewTicker(op.cfg.pruneInterval)
	defer pruneTicker.Stop()
	prepareTicker := time.NewTicker(op.cfg.webRefreshInterval)
	defer prepareTicker.Stop()

	var exitError error
loop:
	for {
		select {
		case <-ctx.Done():
			exitError = ctx.Err()
			break loop

		case ev, ok := <-events:
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
		case <-pruneTicker.C:
			op.prune()
		case <-prepareTicker.C:
			if err := op.server.prepareAll(); err != nil {
				op.log.Error("preparing web data failed", "err", err)
			}

		}
	}

	cancel()
	op.log.Debug("hostname operator done")
	return exitError
}

func (op *hostnameOperator) prepareIgnoreListData(pd *preparedResult) error {
	op.log.Debug("preparing ignore-list")
	buf := &bytes.Buffer{}
	data := make(map[string]interface{})

	err := op.leasesIgnored.each(func(leaseID mtypes.LeaseID, lastError error, failedAt time.Time, count uint, extra ...string) error {
		preparedEntry := struct {
			Hostnames     []string `json:"hostnames"`
			LastError     string   `json:"last-error"`
			LastErrorType string   `json:"last-error-type"`
			FailedAt      string   `json:"failed-at"`
			FailureCount  uint     `json:"failure-count"`
			Namespace     string   `json:"namespace"`
		}{
			LastError:     lastError.Error(),
			LastErrorType: fmt.Sprintf("%T", lastError),
			FailedAt:      failedAt.UTC().String(),
			FailureCount:  count,
			Namespace:     clusterutil.LeaseIDToNamespace(leaseID),
		}

		for _, hostname := range extra {
			preparedEntry.Hostnames = append(preparedEntry.Hostnames, hostname)
		}

		data[leaseID.String()] = preparedEntry
		return nil
	})

	if err != nil {
		return err
	}

	enc := json.NewEncoder(buf)
	err = enc.Encode(data)
	if err != nil {
		return err
	}

	pd.set(buf.Bytes())
	return nil
}

func (op *hostnameOperator) prepareHostnamesData(pd *preparedResult) error {
	op.log.Debug("preparing managed-hostnames")
	buf := &bytes.Buffer{}
	data := make(map[string]interface{})

	for hostname, entry := range op.hostnames {
		preparedEntry := struct {
			LeaseID      mtypes.LeaseID
			Namespace    string
			ExternalPort uint32
			ServiceName  string
			LastUpdate   string
		}{
			LeaseID:      entry.presentLease,
			Namespace:    clusterutil.LeaseIDToNamespace(entry.presentLease),
			ExternalPort: entry.presentExternalPort,
			ServiceName:  entry.presentServiceName,
			LastUpdate:   entry.lastChangeAt.String(),
		}
		data[hostname] = preparedEntry
	}

	enc := json.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return err
	}

	pd.set(buf.Bytes())
	return nil
}

func (op *hostnameOperator) prune() {
	if op.leasesIgnored.prune() {
		op.flagIgnoreListData()
	}

}

func (op *hostnameOperator) recordEventError(ev ctypes.HostnameResourceEvent, failure error) {
	// ff no error, no action
	if failure == nil {
		return
	}

	// check the error, only consider errors that are obviously
	// indicating a missing resource
	// otherwise simple errors like network issues could wind up with all CRDs
	// being ignored

	mark := false

	if kubeErrors.IsNotFound(failure) {
		mark = true
	}

	if errors.Is(failure, errExpectedResourceNotFound) {
		mark = true
	}

	errStr := failure.Error()
	// unless the error indicates a resource was not found, no action
	if strings.Contains(errStr, "not found") {
		mark = true
	}

	if !mark {
		return
	}

	op.log.Info("recording error for", "lease", ev.GetLeaseID().String(), "err", failure)

	op.leasesIgnored.addError(ev.GetLeaseID(), failure, ev.GetHostname())
	op.flagIgnoreListData()
}

func (op *hostnameOperator) isEventIgnored(ev ctypes.HostnameResourceEvent) bool {
	return op.leasesIgnored.isFlagged(ev.GetLeaseID())
}

func (op *hostnameOperator) applyEvent(ctx context.Context, ev ctypes.HostnameResourceEvent) error {
	op.log.Debug("apply event", "event-type", ev.GetEventType(), "hostname", ev.GetHostname())
	switch ev.GetEventType() {
	case ctypes.ProviderResourceDelete:
		// note that on delete the resource might be gone anyways because the namespace is deleted
		return op.applyDeleteEvent(ctx, ev)
	case ctypes.ProviderResourceAdd, ctypes.ProviderResourceUpdate:
		if op.isEventIgnored(ev) {
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

func (op *hostnameOperator) applyDeleteEvent(ctx context.Context, ev ctypes.HostnameResourceEvent) error {
	leaseID := ev.GetLeaseID()
	err := op.client.RemoveHostnameFromDeployment(ctx, ev.GetHostname(), leaseID, true)

	if err == nil {
		delete(op.hostnames, ev.GetHostname())
		op.flagHostnamesData()
	}

	return err
}

func buildDirective(ev ctypes.HostnameResourceEvent, serviceExpose crd.ManifestServiceExpose) ctypes.ConnectHostnameToDeploymentDirective {
	// Build the directive based off the event
	directive := ctypes.ConnectHostnameToDeploymentDirective{
		Hostname:    ev.GetHostname(),
		LeaseID:     ev.GetLeaseID(),
		ServiceName: ev.GetServiceName(),
		ServicePort: int32(ev.GetExternalPort()),
	}
	/*
		Populate the configuration options
		selectedExpose.HttpOptions has zero values if this is from an earlier CRD. Just insert
		defaults and move on
	*/
	if serviceExpose.HTTPOptions.MaxBodySize == 0 {
		directive.ReadTimeout = 60000
		directive.SendTimeout = 60000
		directive.NextTimeout = 60000
		directive.MaxBodySize = 1048576
		directive.NextTries = 3
		directive.NextCases = []string{"error", "timeout"}
	} else {
		directive.ReadTimeout = serviceExpose.HTTPOptions.ReadTimeout
		directive.SendTimeout = serviceExpose.HTTPOptions.SendTimeout
		directive.NextTimeout = serviceExpose.HTTPOptions.NextTimeout
		directive.MaxBodySize = serviceExpose.HTTPOptions.MaxBodySize
		directive.NextTries = serviceExpose.HTTPOptions.NextTries
		directive.NextCases = serviceExpose.HTTPOptions.NextCases
	}

	return directive
}

func locateServiceFromManifest(ctx context.Context, client cluster.Client, leaseID mtypes.LeaseID, serviceName string, externalPort uint32) (crd.ManifestServiceExpose, error) {

	// Locate the matchin service name & expose directive in the manifest CRD
	found, manifestGroup, err := client.GetManifestGroup(ctx, leaseID)
	if err != nil {
		return crd.ManifestServiceExpose{}, err
	}
	if !found {
		/*
			It's possible this code could race to read the CRD, although unlikely. If this fails the operator
			restarts and should work the attempt anyways. If this becomes a pain point then the operator
			can be rewritten to watch for CRD events on the manifest as well, then avoid running this code
			until the manifest exists.
		*/
		return crd.ManifestServiceExpose{}, fmt.Errorf("%w: manifest for %v", errExpectedResourceNotFound, leaseID)
	}

	var selectedService crd.ManifestService
	for _, service := range manifestGroup.Services {
		if service.Name == serviceName {
			selectedService = service
			break
		}
	}

	if selectedService.Count == 0 {
		return crd.ManifestServiceExpose{}, fmt.Errorf("%w: service for %v - %v", errExpectedResourceNotFound, leaseID, serviceName)
	}

	var selectedExpose crd.ManifestServiceExpose
	for _, expose := range selectedService.Expose {
		if !expose.Global {
			continue
		}

		if externalPort == uint32(util.ExposeExternalPort(manifest.ServiceExpose{
			Port:         expose.Port,
			ExternalPort: expose.ExternalPort,
		})) {
			selectedExpose = expose
			break
		}
	}

	if selectedExpose.Port == 0 {
		return crd.ManifestServiceExpose{}, fmt.Errorf("%w: service expose for %v - %v - %v", errExpectedResourceNotFound, leaseID, serviceName, externalPort)
	}

	return selectedExpose, nil
}

func (op *hostnameOperator) applyAddOrUpdateEvent(ctx context.Context, ev ctypes.HostnameResourceEvent) error {
	selectedExpose, err := locateServiceFromManifest(ctx, op.client, ev.GetLeaseID(), ev.GetServiceName(), ev.GetExternalPort())
	if err != nil {
		return err
	}

	leaseID := ev.GetLeaseID()

	op.log.Debug("connecting",
		"hostname", ev.GetHostname(),
		"lease", leaseID,
		"service", ev.GetServiceName(),
		"externalPort", ev.GetExternalPort())
	entry, exists := op.hostnames[ev.GetHostname()]

	isSameLease := false
	if exists {
		isSameLease = entry.presentLease.Equals(leaseID)
	} else {
		isSameLease = true
	}

	directive := buildDirective(ev, selectedExpose)

	if isSameLease {
		shouldConnect := false

		if !exists {
			shouldConnect = true
			op.log.Debug("hostname target is new, applying")
			// Check to see if port or service name is different
		} else if entry.presentExternalPort != ev.GetExternalPort() || entry.presentServiceName != ev.GetServiceName() {
			shouldConnect = true
			op.log.Debug("hostname target has changed, applying")
		}

		if shouldConnect {
			op.log.Debug("Updating ingress")
			// Update or create the existing ingress
			err = op.client.ConnectHostnameToDeployment(ctx, directive)
		}
	} else {
		op.log.Debug("Swapping ingress to new deployment")
		//  Delete the ingress in one namespace and recreate it in the correct one
		err = op.client.RemoveHostnameFromDeployment(ctx, ev.GetHostname(), entry.presentLease, false)
		if err == nil {
			err = op.client.ConnectHostnameToDeployment(ctx, directive)
		}
	}

	if err == nil { // Update sored entry if everything went OK
		entry.presentExternalPort = ev.GetExternalPort()
		entry.presentServiceName = ev.GetServiceName()
		entry.presentLease = leaseID
		entry.lastEvent = ev
		entry.lastChangeAt = time.Now()
		op.hostnames[ev.GetHostname()] = entry
		op.flagHostnamesData()
	}

	return err
}

func newHostnameOperator(logger log.Logger, client cluster.Client, config hostnameOperatorConfig) (*hostnameOperator) {

	op := &hostnameOperator{
		hostnames:  make(map[string]managedHostname),
		client:     client,
		log:        logger,
		cfg:        config,
		server: newOperatorHttp(),
		leasesIgnored: newIgnoreList(ignoreListConfig{ // TODO - should be a param
			failureLimit: config.eventFailureLimit,
			entryLimit:   config.ignoreListEntryLimit,
			ageLimit:     config.ignoreListAgeLimit,
		}),
	}

	op.flagIgnoreListData = op.server.addPreparedEndpoint("/ignore-list", op.prepareIgnoreListData)
	op.flagHostnamesData = op.server.addPreparedEndpoint("/managed-hostnames",op.prepareHostnamesData)

	return op
}

func doHostnameOperator(cmd *cobra.Command) error {
	ns := viper.GetString(FlagK8sManifestNS)

	listenAddr := viper.GetString(FlagListenAddress)
	config := hostnameOperatorConfig{
		listenAddress:        viper.GetString(FlagListenAddress),
		pruneInterval:        viper.GetDuration(FlagPruneInterval),
		ignoreListEntryLimit: viper.GetUint(FlagIgnoreListEntryLimit),
		webRefreshInterval:   viper.GetDuration(FlagWebRefreshInterval),
		retryDelay:           viper.GetDuration(FlagRetryDelay),
		ignoreListAgeLimit:   viper.GetDuration(FlagIgnoreListAgeLimit),
		eventFailureLimit:    viper.GetUint(FlagEventFailureLimit),
	}

	logger := openLogger()

	// Config path not provided because the authorization comes from the role assigned to the deployment
	// and provided by kubernetes
	client, err := clusterClient.NewClient(logger, ns, "")
	if err != nil {
		return err
	}

	op := newHostnameOperator(logger, client, config)

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

func addOperatorFlags(cmd *cobra.Command) {
	cmd.Flags().String(FlagK8sManifestNS, "lease", "Cluster manifest namespace")
	if err := viper.BindPFlag(FlagK8sManifestNS, cmd.Flags().Lookup(FlagK8sManifestNS)); err != nil {
		panic(err)
	}

	cmd.Flags().String(FlagListenAddress, "0.0.0.0:8085", "listen address for web server")
	if err := viper.BindPFlag(FlagListenAddress, cmd.Flags().Lookup(FlagListenAddress)); err != nil {
		panic(err)
	}

	cmd.Flags().Duration(FlagPruneInterval, 10*time.Minute, "data pruning interval")
	if err := viper.BindPFlag(FlagPruneInterval, cmd.Flags().Lookup(FlagPruneInterval)); err != nil {
		panic(err)
	}

	cmd.Flags().Uint(FlagIgnoreListEntryLimit, 131072, "ignore list size limit")
	if err := viper.BindPFlag(FlagIgnoreListEntryLimit, cmd.Flags().Lookup(FlagIgnoreListEntryLimit)); err != nil {
		panic(err)
	}

	cmd.Flags().Duration(FlagIgnoreListAgeLimit, time.Hour*726, "ignore list entry age limit")
	if err := viper.BindPFlag(FlagIgnoreListAgeLimit, cmd.Flags().Lookup(FlagIgnoreListAgeLimit)); err != nil {
		panic(err)
	}

	cmd.Flags().Duration(FlagWebRefreshInterval, 5*time.Second, "web data refresh interval")
	if err := viper.BindPFlag(FlagWebRefreshInterval, cmd.Flags().Lookup(FlagWebRefreshInterval)); err != nil {
		panic(err)
	}

	cmd.Flags().Duration(FlagRetryDelay, 3*time.Second, "retry delay")
	if err := viper.BindPFlag(FlagRetryDelay, cmd.Flags().Lookup(FlagRetryDelay)); err != nil {
		panic(err)
	}

	cmd.Flags().Uint(FlagEventFailureLimit, 3, "event failure limit before it is ignored")
	if err := viper.BindPFlag(FlagEventFailureLimit, cmd.Flags().Lookup(FlagEventFailureLimit)); err != nil {
		panic(err)
	}
}

func HostnameOperatorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "hostname-operator",
		Short:        "",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return doHostnameOperator(cmd)
		},
	}
	addOperatorFlags(cmd)

	return cmd
}
