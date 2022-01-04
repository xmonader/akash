package util

import atypes "github.com/ovrclk/akash/types/v1beta2"

func GetEndpointQuantity(resources atypes.ResourceGroup, kind atypes.Endpoint_Kind) uint {
	endpoints := make(map[uint32]struct{})
	for _, resource := range resources.GetResources() {
		for _, endpoint := range resource.Resources.Endpoints {
			if endpoint.Kind == kind {
				endpoints[endpoint.SequenceNumber] = struct{}{}
			}
		}
	}
	return uint(len(endpoints))
}