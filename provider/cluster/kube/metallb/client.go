package metallb

import (
	"fmt"
	"github.com/ovrclk/akash/provider/cluster/kube/client_common"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/flowcontrol"
	"github.com/tendermint/tendermint/libs/log"
)

type Client interface {
	GetIPAddressCount() (uint, error)
	GetIPAddressInUseCount() (uint, error)
}

type client struct {
	kube kubernetes.Interface
}

func NewClient(configPath string, log log.Logger) (Client, error){
	config, err := client_common.OpenKubeConfig(configPath, log)
	if err != nil {
		return nil, fmt.Errorf("%w: creating kubernetes client", err)
	}
	config.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()

	kc, err := kubernetes.NewForConfig(config)

	if err != nil {
		return nil, fmt.Errorf("%w: creating kubernetes client", err)
	}

	return &client{
		kube: kc,
	}, nil

}

/*
can get stuff like this to access metal lb metrics
   75  nslookup -type=SRV _monitoring._tcp.controller.metallb-system.svc.cluster.local

  102  curl -I controller.metallb-system.svc.cluster.local:7472/metrics

 */

func (c *client) GetIPAddressCount() (uint, error) {
	return 0, nil
}

func (c *client) GetIPAddressInUseCount() (uint, error) {
	return 0, nil
}
