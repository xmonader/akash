package operatorcommon

import (
	"net/http"
	"sync/atomic"
	"time"
)

type PreparedResult interface {
	Flag()
	Set([]byte)
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
	result.Set([]byte{})
	return result
}

func (pr *preparedResult) Flag() {
	pr.needsPrepare = true
}

func (pr *preparedResult) Set(data []byte) {
	pr.needsPrepare = false
	pr.data.Store(preparedResultData{
		preparedAt: time.Now(),
		data:       data,
	})
}

func (pr *preparedResult) get() preparedResultData {
	return (pr.data.Load()).(preparedResultData)
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
