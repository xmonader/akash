package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	ctypes "github.com/ovrclk/akash/provider/cluster/types"
	clusterutil "github.com/ovrclk/akash/provider/cluster/util"
	mtypes "github.com/ovrclk/akash/x/market/types/v1beta2"
	"sync/atomic"
	"time"
)

type managedHostname struct {
	lastEvent    ctypes.HostnameResourceEvent
	presentLease mtypes.LeaseID

	presentServiceName  string
	presentExternalPort uint32
	lastChangeAt        time.Time
}

type preparedResultData struct {
	preparedAt time.Time
	data       []byte
}

type preparedResult struct {
	needsPrepare bool
	data         atomic.Value
}

func newPreparedResult() *preparedResult {
	result := &preparedResult{
		needsPrepare: true,
	}
	result.set([]byte{})
	return result
}

func (pr *preparedResult) flag() {
	pr.needsPrepare = true
}

func (pr *preparedResult) set(data []byte) {
	pr.needsPrepare = false
	pr.data.Store(preparedResultData{
		preparedAt: time.Now(),
		data:       data,
	})
}

func (pr *preparedResult) get() preparedResultData {
	return (pr.data.Load()).(preparedResultData)
}

// TODO - move me to common file
type ignoreListEntry struct {
	failureCount uint
	failedAt     time.Time
	lastError    error

	extra    map[string]struct{}
}

type ignoreListConfig struct {
	failureLimit uint
	entryLimit uint
	ageLimit time.Duration
}

type ignoreList struct {
	entries map[mtypes.LeaseID]ignoreListEntry
	cfg ignoreListConfig
}

func newIgnoreList(config ignoreListConfig) *ignoreList{
	return &ignoreList{
		entries: make(map[mtypes.LeaseID]ignoreListEntry),
		cfg:     config,
	}
}

func (il *ignoreList) prepare(pd *preparedResult) error {
	data := make(map[string]interface{})

	err := il.each(func(leaseID mtypes.LeaseID, lastError error, failedAt time.Time, count uint, extra ...string) error {
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

	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err = enc.Encode(data)
	if err != nil {
		return err
	}

	pd.set(buf.Bytes())
	return nil

}

func (il *ignoreList) size() int {
	return len(il.entries)
}

func (il *ignoreList) each(f func(k mtypes.LeaseID, failure error, failedAt time.Time, count uint, extra ...string) error ) error {
	for k, v := range il.entries {
		var extras []string
		for extra, _ := range v.extra {
			extras = append(extras, extra)
		}
		err := f(k, v.lastError, v.failedAt, v.failureCount, extras...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (il *ignoreList) addError(k mtypes.LeaseID, failure error, extra ...string) {
	// Increment the error counter
	entry := il.entries[k]
	entry.failureCount++
	entry.failedAt = time.Now()
	entry.lastError = failure

	for _, v := range extra {
		if entry.extra == nil {
			entry.extra = make(map[string]struct{})
		}
		entry.extra[v] = struct{}{}
	}

	// Store updated copy back into map
	il.entries[k] = entry
}

func (il *ignoreList) getFailureCount(k mtypes.LeaseID) uint {
	return il.entries[k].failureCount
}

func (il *ignoreList) isFlagged(k mtypes.LeaseID) bool {
	entry, ok := il.entries[k]
	if !ok {
		return false
	}

	return entry.failureCount >= il.cfg.entryLimit
}

func (il *ignoreList) prune() bool {
	deleted := false
	// do not let the ignore list grow unbounded, it would eventually
	// consume 100% of available memory otherwise
	if len(il.entries) > int(il.cfg.entryLimit) {
		var toDelete []mtypes.LeaseID

		for leaseID, entry := range il.entries {
			if time.Since(entry.failedAt) > il.cfg.ageLimit {
				toDelete = append(toDelete, leaseID)
			}
		}

		// if enough entries have not been selected for deletion
		// then just remove half of the entries
		if len(il.entries)-len(toDelete) > int(il.cfg.entryLimit) {
	//		op.log.Info("removing half of ignore list entries")
			i := 0
			for leaseID := range il.entries {
				if (i % 2) == 0 {
					toDelete = append(toDelete, leaseID)
				}
				i++
			}
		}

		for _, leaseID := range toDelete {
			//op.log.Info("removing ignore list entry", "lease", leaseID.String())
			delete(il.entries, leaseID)
			deleted = true
		}
	}

	return deleted
}


type hostnameOperatorConfig struct {
	listenAddress        string
	pruneInterval        time.Duration
	ignoreListEntryLimit uint
	ignoreListAgeLimit   time.Duration
	webRefreshInterval   time.Duration
	retryDelay           time.Duration
	eventFailureLimit    uint
}
