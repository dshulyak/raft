package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"path"

	"github.com/dshulyak/raft"
	"github.com/gorilla/mux"
)

const (
	clientURL = "ClientURL"
)

func registerServer(srv *server, router *mux.Router) {
	router.HandleFunc("/get/{key}", srv.Get).Methods(http.MethodGet)
	router.HandleFunc("/write/{key}", srv.Write).Methods(http.MethodPost, http.MethodPut)
}

type server struct {
	raft *raft.Node
	app  *kv
}

func handleError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, raft.ErrProposalsOverflow) {
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	} else if errors.Is(err, raft.ErrStopped) {
		http.Error(w, err.Error(), http.StatusGone)
		return
	} else if errors.Is(err, raft.ErrLeaderStepdown) {
		http.Error(w, err.Error(), http.StatusFailedDependency)
		return
	} else {
		redirect := &raft.ErrRedirect{}
		if errors.As(err, &redirect) {
			node := redirect.Leader
			for i := range node.Info {
				item := &node.Info[i]
				if item.Key == clientURL {
					vars := mux.Vars(r)
					w.Header().Set(
						"Location",
						path.Join(item.Value, "get", vars["key"]),
					)
					http.Error(w, "Redirect to the current leader.",
						http.StatusFound)
				}
			}
		}
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (s *server) Get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if len(key) == 0 {
		http.Error(w, "Key shouldn't be empty", http.StatusPreconditionFailed)
		return
	}
	req, err := s.raft.Read(r.Context())
	if err != nil {
		handleError(w, r, err)
		return
	}
	if err = req.Wait(r.Context()); err != nil {
		handleError(w, r, err)
		return
	}
	val, set := s.app.Get(key)
	if !set {
		http.Error(w, "Not set", http.StatusNotFound)
		return
	}
	_, _ = w.Write([]byte(val))
}

func (s *server) Write(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	if len(key) == 0 {
		http.Error(w, "Key shouldn't be empty", http.StatusPreconditionFailed)
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body.", http.StatusBadRequest)
		return
	}
	op, err := s.app.cdc.write(key, string(data))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req, err := s.raft.Propose(r.Context(), op)
	if err != nil {
		handleError(w, r, err)
		return
	}
	if err = req.Wait(r.Context()); err != nil {
		handleError(w, r, err)
		return
	}
	rst, err := req.WaitResult(r.Context())
	if err != nil {
		handleError(w, r, err)
		return
	}
	_ = json.NewEncoder(w).Encode(rst)
}
