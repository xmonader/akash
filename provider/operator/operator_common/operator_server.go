package operator_common

import (
	"github.com/gorilla/mux"
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

func NewOperatorHttp() OperatorHttp {
	retval := &operatorHttp{
		router:  mux.NewRouter(),
		results: make(map[string]preparedEntry),
	}

	retval.router.HandleFunc("/health", func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(rw, "OK")
	})

	retval.router.HandleFunc("/version", func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		// TODO - write the version back in the standardized way from Arijit's Pr
		io.WriteString(rw, "0.0.0")
	}).Methods("GET")

	return retval
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
