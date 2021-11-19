package kube

import (
	"fmt"
	"github.com/ovrclk/akash/provider/cluster/kube/builder"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	"strconv"
)

func recoverLeaseIdFromLabels(labels map[string]string) (mtypes.LeaseID, error) {

	dseqS, ok := labels[builder.AkashLeaseDSeqLabelName]
	if !ok {
		return mtypes.LeaseID{}, fmt.Errorf("%w: %q", ErrMissingLabel, builder.AkashLeaseDSeqLabelName)
	}
	gseqS, ok := labels[builder.AkashLeaseGSeqLabelName]
	if !ok {
		return mtypes.LeaseID{}, fmt.Errorf("%w: %q", ErrMissingLabel, builder.AkashLeaseGSeqLabelName)
	}
	oseqS, ok := labels[builder.AkashLeaseOSeqLabelName]
	if !ok {
		return mtypes.LeaseID{},  fmt.Errorf("%w: %q", ErrMissingLabel, builder.AkashLeaseOSeqLabelName)
	}
	owner, ok := labels[builder.AkashLeaseOwnerLabelName]
	if !ok {
		return mtypes.LeaseID{}, fmt.Errorf("%w: %q", ErrMissingLabel, builder.AkashLeaseOwnerLabelName)
	}

	provider, ok := labels[builder.AkashLeaseProviderLabelName]
	if !ok {
		return mtypes.LeaseID{}, fmt.Errorf("%w: %q", ErrMissingLabel, builder.AkashLeaseProviderLabelName)
	}

	dseq, err := strconv.ParseUint(dseqS, 10, 64)
	if err != nil {
		return mtypes.LeaseID{},  fmt.Errorf("%w: dseq %q not a uint", ErrInvalidLabelValue, dseqS)
	}

	gseq, err := strconv.ParseUint(gseqS, 10, 32)
	if err != nil {
		return mtypes.LeaseID{}, fmt.Errorf("%w: gseq %q not a uint", ErrInvalidLabelValue, gseqS)
	}

	oseq, err := strconv.ParseUint(oseqS, 10, 32)
	if err != nil {
		return mtypes.LeaseID{},  fmt.Errorf("%w: oesq %q not a uint", ErrInvalidLabelValue, oseqS)
	}

	return mtypes.LeaseID{
		Owner:    owner,
		DSeq:     dseq,
		GSeq:     uint32(gseq),
		OSeq:     uint32(oseq),
		Provider: provider,
	}, nil
}
