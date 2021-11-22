package cmd

import (
	"github.com/gorilla/mux"
	"net/http"
)

type prepareFlagFn func ()
type prepareFn func (pd *preparedResult) error
type preparedEntry struct{
	data *preparedResult
	prepare prepareFn
}

type operatorHttp struct {
	router *mux.Router
	results map[string]preparedEntry
}

func newOperatorHttp() *operatorHttp {
	retval := &operatorHttp{
		router: mux.NewRouter(),
		results: make(map[string]preparedEntry),
	}

	// TODO - install standardized version & version-extended endpoints
	retval.router.HandleFunc("/version", func(rw http.ResponseWriter, req *http.Request){
		rw.WriteHeader(http.StatusOK)
		// TODO - write the version back
	}).Methods("GET")

	return retval
}

func (opHttp *operatorHttp) addPreparedEndpoint(path string, prepare prepareFn) prepareFlagFn {
	_ ,exists := opHttp.results[path]
	if exists {
		panic("prepared result exists for path: " + path)
	}

	entry := preparedEntry{
		data: newPreparedResult(),
		prepare: prepare,
	}
	opHttp.results[path] = entry

	opHttp.router.HandleFunc(path, func(rw http.ResponseWriter, req *http.Request){
		servePreparedResult(rw, entry.data)
	}).Methods("GET")

	return entry.data.flag
}

func (opHttp *operatorHttp) prepareAll() error {
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
