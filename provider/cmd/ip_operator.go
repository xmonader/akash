package cmd

import (
	"context"

	"errors"
	"fmt"
	"github.com/ovrclk/akash/provider/cluster"
	clusterClient "github.com/ovrclk/akash/provider/cluster/kube"
	ctypes "github.com/ovrclk/akash/provider/cluster/types"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tendermint/tendermint/libs/log"

	"time"
)

type managedIp struct {
	presentLease mtypes.LeaseID
	presentServiceName string
	lastEvent ctypes.IPResourceEvent
}

type ipOperator struct {
	state map[string]managedIp

	client cluster.Client

	log log.Logger
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

	for _, ipPassThrough := range entries {
		k := getStateKey(ipPassThrough.GetLeaseID(), ipPassThrough.GetSharingKey(), ipPassThrough.GetExternalPort())
		op.state[k] = managedIp{
			presentLease:       ipPassThrough.GetLeaseID(),
			presentServiceName: ipPassThrough.GetSharingKey(),
			lastEvent:          nil,
		}
	}

	events, err := op.client.ObserveIPState(ctx)
	if err != nil {
		cancel()
		return err
	}

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
		}
	}

	cancel()
	op.log.Debug("ip operator done")
	return exitError
}

func (op *ipOperator) applyEvent(ctx context.Context, ev ctypes.IPResourceEvent) error {
	op.log.Debug("apply event", "event-type", ev.GetEventType(), "lease", ev.GetLeaseID())
	switch ev.GetEventType() {
	case ctypes.ProviderResourceDelete:
		// note that on delete the resource might be gone anyways because the namespace is deleted
		return op.applyDeleteEvent(ctx, ev)
	case ctypes.ProviderResourceAdd, ctypes.ProviderResourceUpdate:
		return op.applyAddOrUpdateEvent(ctx, ev)
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

func (op *ipOperator) applyAddOrUpdateEvent(ctx context.Context, ev ctypes.IPResourceEvent) error {
	leaseID := ev.GetLeaseID()

	uid := getStateKeyFromEvent(ev)

	op.log.Debug("connecting",
		"lease", leaseID,
		"service", ev.GetServiceName(),
		"externalPort", ev.GetExternalPort())
	entry, exists := op.state[uid]

	isSameLease := false
	if exists {
		isSameLease = entry.presentLease.Equals(leaseID)
	} else {
		isSameLease = true
	}

	directive := buildIPDirective(ev)

	var err error
	if isSameLease {
		shouldConnect := false

		if !exists {
			shouldConnect = true
			op.log.Debug("ip passthrough is new, applying")
			// Check to see if port or service name is different
		} else if entry.presentServiceName != ev.GetServiceName() {
			shouldConnect = true
			op.log.Debug("ip passthrough has changed, applying")
		}

		if shouldConnect {
			op.log.Debug("Updating ip passthrough")
			err = op.client.CreateIPPassthrough(ctx, leaseID, directive)
		}
	} else {
		op.log.Debug("Swapping ip passthrough to new deployment")

		err = op.client.PurgeIPPassthrough(ctx, leaseID, directive)

		if err == nil {
			err = op.client.CreateIPPassthrough(ctx, leaseID, directive)
		}
	}

	if err == nil { // Update stored entry if everything went OK
		entry.presentServiceName = ev.GetServiceName()
		entry.presentLease = leaseID
		entry.lastEvent = ev
		op.state[uid] = entry
	}

	return err
}

func doIPOperator(cmd *cobra.Command) error {
	ns := viper.GetString(FlagK8sManifestNS)
	logger := openLogger()

	// Config path not provided because the authorization comes from the role assigned to the deployment
	// and provided by kubernetes
	client, err := clusterClient.NewClient(logger, ns, "")
	if err != nil {
		return err
	}

	op := &ipOperator{
		state:  make(map[string]managedIp),
		client: client,
		log:    logger,
	}



	return op.run(cmd.Context())
}

func IPOperatorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "ip-operator",
		Short:        "",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return doIPOperator(cmd)
		},
	}

	cmd.Flags().String(FlagK8sManifestNS, "lease", "Cluster manifest namespace")
	if err := viper.BindPFlag(FlagK8sManifestNS, cmd.Flags().Lookup(FlagK8sManifestNS)); err != nil {
		return nil
	}

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
