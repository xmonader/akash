package kube

import (
	"context"
	"fmt"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/ovrclk/akash/manifest"
	akashtypes "github.com/ovrclk/akash/pkg/apis/akash.network/v1"
	"github.com/ovrclk/akash/provider/cluster/kube/builder"
	ctypes "github.com/ovrclk/akash/provider/cluster/types"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	corev1 "k8s.io/api/core/v1"

	kubeErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/pager"
	"strings"
)

// TODO - these should disappear after a refactor
const (
	akashServiceTarget = "akash.network/service-target"
	akashMetalLB = "metal-lb"
	metalLbAllowSharedIp = "metallb.universe.tf/allow-shared-ip"
)

func ipResourceName(leaseID mtypes.LeaseID, serviceName string, externalPort uint32, proto manifest.ServiceProtocol) string {
	ns := builder.LidNS(leaseID)[0:20]
	resourceName := fmt.Sprintf("%s-%s-%d-%s", ns, serviceName, externalPort, proto)
	return strings.ToLower(resourceName)
}

func (c *client) PurgeDeclaredIP(ctx context.Context, leaseID mtypes.LeaseID, serviceName string, externalPort uint32, proto manifest.ServiceProtocol) error {
	resourceName := ipResourceName(leaseID, serviceName, externalPort, proto)
	return c.ac.AkashV1().ProviderLeasedIPs(c.ns).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=true", builder.AkashManagedLabelName),
		FieldSelector: fmt.Sprintf("metadata.name=%s", resourceName),
	})
}

func (c *client) DeclareIP(ctx context.Context, lID mtypes.LeaseID, serviceName string, port uint32, externalPort uint32, proto manifest.ServiceProtocol, sharingKey string) error {
	resourceName := ipResourceName(lID, serviceName, externalPort, proto)

	labels := map[string]string{
		builder.AkashManagedLabelName: "true",
	}
	builder.AppendLeaseLabels(lID, labels)
	foundEntry, err := c.ac.AkashV1().ProviderLeasedIPs(c.ns).Get(ctx, resourceName, metav1.GetOptions{})

	exists := true
	if err != nil {
		if kubeErrors.IsNotFound(err) {
			exists = false
		} else {
			return err
		}
	}

	obj := akashtypes.ProviderLeasedIP{
		ObjectMeta: metav1.ObjectMeta{
			Name:            resourceName,
			Labels:          labels,
		},
		Spec: akashtypes.ProviderLeasedIPSpec{
			LeaseID:      akashtypes.LeaseIDFromAkash(lID),
			ServiceName:  serviceName,
			ExternalPort: externalPort,
			SharingKey:   sharingKey,
			Protocol: proto.ToString(),
			Port: port,
		},
		Status: akashtypes.ProviderLeasedIPStatus{},
	}

	c.log.Info("declaring leased ip", "lease", lID,
		"service-name", serviceName,
		"port", port,
		"external-port", externalPort,
		"sharing-key", sharingKey,
		"exists", exists)
	// Create or update the entry
	if exists {
		obj.ObjectMeta.ResourceVersion = foundEntry.ResourceVersion
		_, err = c.ac.AkashV1().ProviderLeasedIPs(c.ns).Update(ctx, &obj, metav1.UpdateOptions{})
	} else {
		_, err = c.ac.AkashV1().ProviderLeasedIPs(c.ns).Create(ctx, &obj, metav1.CreateOptions{})
	}

	return err
}

func (c *client) PurgeDeclaredIPs(ctx context.Context, lID mtypes.LeaseID) error {
	labelSelector := &strings.Builder{}
	_, err := fmt.Fprintf(labelSelector, "%s=true,", builder.AkashManagedLabelName)
	if err != nil {
		return err
	}
	kubeSelectorForLease(labelSelector, lID)
	result := c.ac.AkashV1().ProviderLeasedIPs(c.ns).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	})

	return result
}

func (c *client) GetIPPassthroughs(ctx context.Context) ([]ctypes.IPPassthrough, error) {
	servicePager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error){
		return c.kc.CoreV1().Services(metav1.NamespaceAll).List(ctx, opts)
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
	
	result := make([]ctypes.IPPassthrough, 0)
	err = servicePager.EachListItem(ctx,
		metav1.ListOptions{
			LabelSelector: labelSelector.String(),
		},
		func(obj runtime.Object) error {
			service := obj.(*corev1.Service)

			_, hasOwner := service.ObjectMeta.Labels[builder.AkashLeaseOwnerLabelName]
			if !hasOwner {
				// Not a service related to a running deployment, so probably internal services
				return nil
			}

			if service.Spec.Type != corev1.ServiceTypeLoadBalancer {
				return fmt.Errorf("resource %q wrong type in service definition %v", service.ObjectMeta.Name, service.Spec.Type)
			}


			ports := service.Spec.Ports
			const expectedNumberOfPorts = 1
			if len(ports) != expectedNumberOfPorts {
				return fmt.Errorf("resource %q  wrong number of ports in load balancer service definition. expected %d, got %d", service.ObjectMeta.Name, expectedNumberOfPorts, len(ports))
			}

			portDefn := ports[0]
			proto := portDefn.Protocol
			port := portDefn.Port

			// TODO - use a utlity method here rather than a cast
			mproto, err := manifest.ParseServiceProtocol(string(proto))
			if err != nil {
				return err // TODO include resource name
			}

			leaseID, err := recoverLeaseIdFromLabels(service.Labels)
			if err != nil {
				return err // TODO include resource name
			}


			serviceSelector := service.Spec.Selector
			serviceName := serviceSelector[builder.AkashManifestServiceLabelName]
			if len(serviceName) == 0 {
				return fmt.Errorf("service name cannot be empty")
			}

			sharingKey := service.ObjectMeta.Annotations[metalLbAllowSharedIp]

			v := ipResourceEvent{
				lID:          leaseID,
				eventType:    "", // unused
				serviceName:  serviceName,
				externalPort: uint32(port),
				sharingKey:   sharingKey,
				providerAddr: nil, // unused
				ownerAddr:    nil, // unused
				protocol:     mproto,
			}

			result = append(result, v)
			return nil
		})


	return result, err
}

func (c *client) ObserveIPState(ctx context.Context) (<-chan ctypes.IPResourceEvent, error) {
	var lastResourceVersion string
	phpager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
		resources, err := c.ac.AkashV1().ProviderLeasedIPs(c.ns).List(ctx, opts)

		if err == nil && len(resources.GetResourceVersion()) != 0 {
			lastResourceVersion = resources.GetResourceVersion()
		}
		return resources, err
	})

	data := make([]akashtypes.ProviderLeasedIP, 0, 128)
	err := phpager.EachListItem(ctx, metav1.ListOptions{}, func(obj runtime.Object) error {
		plip := obj.(*akashtypes.ProviderLeasedIP)
		data = append(data, *plip)
		return nil
	})

	if err != nil {
		return nil, err
	}

	c.log.Info("starting ip passthrough watch", "resourceVersion", lastResourceVersion)
	watcher, err := c.ac.AkashV1().ProviderLeasedIPs(c.ns).Watch(ctx, metav1.ListOptions{
		TypeMeta:             metav1.TypeMeta{},
		LabelSelector:        "",
		FieldSelector:        "",
		Watch:                false,
		AllowWatchBookmarks:  false,
		ResourceVersion:      lastResourceVersion,
		ResourceVersionMatch: "",
		TimeoutSeconds:       nil,
		Limit:                0,
		Continue:             "",
	})
	if err != nil {
		return nil, err
	}

	evData := make([]ipResourceEvent, len(data))
	for i, v := range data {
		ownerAddr, err := sdktypes.AccAddressFromBech32(v.Spec.LeaseID.Owner)
		if err != nil {
			return nil, err
		}
		providerAddr, err := sdktypes.AccAddressFromBech32(v.Spec.LeaseID.Provider)
		if err != nil {
			return nil, err
		}

		leaseID, err := v.Spec.LeaseID.ToAkash()
		if err != nil {
			return nil, err
		}

		proto, err := manifest.ParseServiceProtocol(v.Spec.Protocol)
		if err != nil {
			return nil, err
		}

		ev := ipResourceEvent{
			eventType:    ctypes.ProviderResourceAdd,
			lID: leaseID,
			serviceName:  v.Spec.ServiceName,
			port: v.Spec.Port,
			externalPort: v.Spec.ExternalPort,
			ownerAddr: ownerAddr,
			providerAddr: providerAddr,
			sharingKey: v.Spec.SharingKey,
			protocol: proto,
		}
		evData[i] = ev
	}

	data = nil

	output := make(chan ctypes.IPResourceEvent)

	go func() {
		defer close(output)
		for _, v := range evData {
			output <- v
		}
		evData = nil // do not hold the reference

		results := watcher.ResultChan()
		for {
			select {
			case result, ok := <-results:
				if !ok { // Channel closed when an error happens
					return
				}
				plip := result.Object.(*akashtypes.ProviderLeasedIP)
				ownerAddr, err := sdktypes.AccAddressFromBech32(plip.Spec.LeaseID.Owner)
				if err != nil {
					c.log.Error("invalid owner address in provider host", "addr", plip.Spec.LeaseID.Owner, "err", err)
					continue // Ignore event
				}
				providerAddr, err := sdktypes.AccAddressFromBech32(plip.Spec.LeaseID.Provider)
				if err != nil {
					c.log.Error("invalid provider address in provider host", "addr", plip.Spec.LeaseID.Provider, "err", err)
					continue // Ignore event
				}
			    leaseID, err := plip.Spec.LeaseID.ToAkash()
			    if err != nil {
			    	c.log.Error("invalid lease ID", "err", err)
					continue // Ignore event
				}
				proto, err := manifest.ParseServiceProtocol(plip.Spec.Protocol)
				if err != nil {
					c.log.Error("invalid protocol", "err", err)
					continue
				}

				ev := ipResourceEvent{
					lID:          leaseID,
					serviceName:  plip.Spec.ServiceName,
					port: plip.Spec.Port,
					externalPort: plip.Spec.ExternalPort,
					sharingKey:   plip.Spec.SharingKey,
					providerAddr: providerAddr,
					ownerAddr:    ownerAddr,
					protocol:     proto,
				}
				switch result.Type {

				case watch.Added:
					ev.eventType = ctypes.ProviderResourceAdd
				case watch.Modified:
					ev.eventType = ctypes.ProviderResourceUpdate
				case watch.Deleted:
					ev.eventType = ctypes.ProviderResourceDelete

				case watch.Error:
					// Based on examination of the implementation code, this is basically never called anyways
					c.log.Error("watch error", "err", result.Object)

				default:
					continue
				}

				output <- ev

			case <-ctx.Done():
				return
			}
		}
	}()

	return output, nil
}

type ipResourceEvent struct {
	lID mtypes.LeaseID
	eventType ctypes.ProviderResourceEvent
	serviceName string
	port uint32
	externalPort uint32
	sharingKey string
	providerAddr sdktypes.Address
	ownerAddr sdktypes.Address
	protocol manifest.ServiceProtocol
}

func (ev ipResourceEvent) GetLeaseID() mtypes.LeaseID {
	return ev.lID
}


func (ev ipResourceEvent) GetEventType() ctypes.ProviderResourceEvent {
	return ev.eventType
}

func (ev ipResourceEvent) GetServiceName() string {
	return ev.serviceName
}

func (ev ipResourceEvent) GetPort() uint32 {
	return ev.port
}

func (ev ipResourceEvent) GetExternalPort() uint32 {
	return ev.externalPort
}

func (ev ipResourceEvent) GetSharingKey() string {
	return ev.sharingKey
}

func (ev ipResourceEvent) GetProtocol() manifest.ServiceProtocol {
	return ev.protocol
}

func (c *client) PurgeIPPassthrough(ctx context.Context, leaseID mtypes.LeaseID, directive ctypes.ClusterIPPassthroughDirective) error {
	ns := builder.LidNS(leaseID)
	portName := createIPPassthroughResourceName(directive)

	err := c.kc.CoreV1().Services(ns).Delete(ctx, portName, metav1.DeleteOptions{})

	if err != nil && kubeErrors.IsNotFound(err) {
		return nil
	}

	return err
}

func createIPPassthroughResourceName(directive ctypes.ClusterIPPassthroughDirective) string{
	return strings.ToLower(fmt.Sprintf("%s-ip-%d-%v", directive.ServiceName, directive.ExternalPort, directive.Protocol))
}

func (c *client) GetIPStatusForLease(ctx context.Context, leaseID mtypes.LeaseID) ([]interface{}, error){
	ns := builder.LidNS(leaseID)
	servicePager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error){
		return c.kc.CoreV1().Services(ns).List(ctx, opts)
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

	err = servicePager.EachListItem(ctx, metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	},
		func(obj runtime.Object) error {
			service := obj.(*corev1.Service)

			loadBalancerIngress := service.Status.LoadBalancer.Ingress
			// Logs something like this : │ load balancer status                         cmp=provider client=kube service=web-ip-80-tcp lb-ingress="[{IP:24.0.0.1 Hostname: Ports:[]}]"
			c.log.Debug("load balancer status", "service", service.ObjectMeta.Name, "lb-ingress", loadBalancerIngress)
			for _, ingress := range loadBalancerIngress {
				_ = ingress.IP
			}

		return nil
		})

	return nil, nil
}

func (c *client) CreateIPPassthrough(ctx context.Context, leaseID mtypes.LeaseID, directive ctypes.ClusterIPPassthroughDirective) error {
	var proto corev1.Protocol

	switch(directive.Protocol) {
	case manifest.TCP:
		proto = corev1.ProtocolTCP
	case manifest.UDP:
		proto = corev1.ProtocolUDP
	default:
		return fmt.Errorf("%w unknown protocol %v", ErrInternalError, directive.Protocol)
	}

	ns := builder.LidNS(leaseID)
	portName := createIPPassthroughResourceName(directive)

	foundEntry, err := c.kc.CoreV1().Services(ns).Get(ctx, portName, metav1.GetOptions{})

	exists := true
	if err != nil {
		if kubeErrors.IsNotFound(err) {
			exists = false
		} else {
			return err
		}
	}

	labels := make(map[string]string)
	builder.AppendLeaseLabels(leaseID, labels)
	labels[builder.AkashManagedLabelName] = "true"
	labels[akashServiceTarget] = akashMetalLB

	selector := map[string]string {
		builder.AkashManagedLabelName: "true",
		builder.AkashManifestServiceLabelName: directive.ServiceName,
	}
	// TODO - specify metallb.universe.tf/address-pool annotation if configured to do so only that pool is used at any time
	annotations := map[string]string {
		metalLbAllowSharedIp: directive.SharingKey,
	}

	port := corev1.ServicePort{
		Name:        portName,
		Protocol:    proto,
		Port:        int32(directive.ExternalPort),
		TargetPort:  intstr.FromInt(int(directive.Port)),
	}

	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       portName,
			Labels: labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				port,
			},
			Selector:                      selector,
			Type:                          corev1.ServiceTypeLoadBalancer,
		},
		Status: corev1.ServiceStatus{},
	}

	c.log.Debug("creating metal-lb service",
		"service", directive.ServiceName,
		"port", directive.Port,
		"external-port", directive.ExternalPort,
		"sharing-key", directive.SharingKey,
		"exists", exists)
	if exists {
		svc.ResourceVersion = foundEntry.ResourceVersion
		_, err = c.kc.CoreV1().Services(ns).Update(ctx, &svc, metav1.UpdateOptions{})
	} else {
		_, err = c.kc.CoreV1().Services(ns).Create(ctx, &svc, metav1.CreateOptions{})	
	}
	
	if err != nil {
		return err
	}

	return nil
}
