package cluster

import (
	ctypes "github.com/ovrclk/akash/provider/cluster/types"
	atypes "github.com/ovrclk/akash/types/v1beta2"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
)

func newReservation(order mtypes.OrderID, resources atypes.ResourceGroup) *reservation {
	endpoints := make(map[uint32]struct{})
	for _, resource := range resources.GetResources() {
		for _, endpoint := range resource.Resources.Endpoints {
			if endpoint.Kind == atypes.Endpoint_LEASED_IP {
				endpoints[endpoint.SequenceNumber] = struct{}{}
			}
		}
	}

	return &reservation{order: order, resources: resources, endpointQuantity: uint(len(endpoints))}
}

type reservation struct {
	order     mtypes.OrderID
	resources atypes.ResourceGroup
	allocated bool
	endpointQuantity uint
}

var _ ctypes.Reservation = (*reservation)(nil)

func (r *reservation) OrderID() mtypes.OrderID {
	return r.order
}

func (r *reservation) Resources() atypes.ResourceGroup {
	return r.resources
}

func (r *reservation) Allocated() bool {
	return r.allocated
}
