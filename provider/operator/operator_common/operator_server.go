package operator_common

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/ovrclk/akash/provider/gateway/utils"
	"io"
	"net/http"
)

type PrepareFlagFn func()
type PrepareFn func(pd PreparedResult) error
type preparedEntry struct {
	data    *preparedResult
	prepare PrepareFn
}

type OperatorHttp interface {
	AddPreparedEndpoint(path string, prepare PrepareFn) PrepareFlagFn
	GetRouter() *mux.Router
	PrepareAll() error
}

type operatorHttp struct {
	router  *mux.Router
	results map[string]preparedEntry
}

func NewOperatorHttp() (OperatorHttp, error) {
	retval := &operatorHttp{
		router:  mux.NewRouter(),
		results: make(map[string]preparedEntry),
	}

	retval.router.HandleFunc("/health", func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(rw, "OK")
	})

	akashVersion :=	utils.NewAkashVersionInfo()
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	err := enc.Encode(akashVersion)

	if err != nil {
		return nil, err
	}

	akashVersionJson := buf.Bytes()
	buf = nil // remove from scope
	enc = nil // remove from scope

	retval.router.HandleFunc("/version", func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = io.Copy(rw, bytes.NewReader(akashVersionJson))
	}).Methods("GET")

	return retval, nil
}

func (opHttp *operatorHttp) GetRouter() *mux.Router {
	return opHttp.router
}

func (opHttp *operatorHttp) AddPreparedEndpoint(path string, prepare PrepareFn) PrepareFlagFn {
	_, exists := opHttp.results[path]
	if exists {
		panic("prepared result exists for path: " + path)
	}

	entry := preparedEntry{
		data:    newPreparedResult(),
		prepare: prepare,
	}
	opHttp.results[path] = entry

	opHttp.router.HandleFunc(path, func(rw http.ResponseWriter, req *http.Request) {
		servePreparedResult(rw, entry.data)
	}).Methods(http.MethodGet)

	return entry.data.Flag
}

func (opHttp *operatorHttp) PrepareAll() error {
	for _, entry := range opHttp.results {
		if !entry.data.needsPrepare {
			continue
		}
		err := entry.prepare(entry.data)
		if err != nil {
			return err
		}
	}

	return nil
}
