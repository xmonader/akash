package rest

import (
	cltypes "github.com/ovrclk/akash/provider/cluster/types"
	ipoptypes "github.com/ovrclk/akash/provider/operator/ip_operator/types"
)

type LeaseStatus struct{
	Services       map[string]*cltypes.ServiceStatus        `json:"services"`
	ForwardedPorts map[string][]cltypes.ForwardedPortStatus `json:"forwarded_ports"` // Container services that are externally accessible
	IPs []ipoptypes.LeaseIPStatus `json:"ips"` // TODO - redo this to be a map like the other entries
}

