// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4 // import "sbinet.org/x/aranet4"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

type Server struct {
	mux *http.ServeMux

	mu    sync.RWMutex
	db    *bbolt.DB
	last  Data
	plots struct {
		CO2     bytes.Buffer
		T, H, P bytes.Buffer
	}
}

func NewServer(root, dbfile string) (*Server, error) {
	db, err := bbolt.Open(dbfile, 0644, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("could not open aranet4 db: %w", err)
	}

	srv := &Server{
		db:  db,
		mux: http.NewServeMux(),
	}

	root = strings.TrimRight(root, "/")
	srv.mux.HandleFunc(root+"/", srv.handleRoot)
	srv.mux.HandleFunc(root+"/favicon.ico", func(w http.ResponseWriter, r *http.Request) {})
	srv.mux.HandleFunc(root+"/post", srv.handleIngest)
	srv.mux.HandleFunc(root+"/plot-co2", srv.handlePlotCO2)
	srv.mux.HandleFunc(root+"/plot-h", srv.handlePlotH)
	srv.mux.HandleFunc(root+"/plot-p", srv.handlePlotP)
	srv.mux.HandleFunc(root+"/plot-t", srv.handlePlotT)

	err = srv.init()
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("could not initialize server: %w", err)
	}

	return srv, nil
}

func (srv *Server) Close() error {
	return srv.db.Close()
}

func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	srv.mux.ServeHTTP(w, r)
}

func (srv *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		fmt.Fprintf(w, "could not parse form: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	cnv := func(name string) int64 {
		v := r.Form.Get(name)
		if v == "" {
			return -1
		}
		vv, err := time.Parse("2006-01-02", v)
		if err != nil {
			return -1
		}
		return vv.UTC().Unix()
	}

	var (
		beg = cnv("from")
		end = cnv("to")
	)

	srv.mu.Lock()
	defer srv.mu.Unlock()

	data, err := srv.rows(beg, end)
	if err != nil {
		fmt.Fprintf(w, "could not read rows from db: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = srv.plot(data)
	if err != nil {
		fmt.Fprintf(w, "could not create plots: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	refresh := int(srv.last.Interval.Seconds())
	if refresh == 0 {
		refresh = 10
	}
	fmt.Fprintf(w, page, refresh, srv.last.String())
}

func (srv *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		err := fmt.Errorf("invalid HTTP method: %s", r.Method)
		log.Printf("%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var (
		vs  []Data
		err = json.NewDecoder(r.Body).Decode(&vs)
	)
	if err != nil {
		err = fmt.Errorf("could not decode JSON payload: %w", err)
		log.Printf("%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = srv.write(vs)
	if err != nil {
		err = fmt.Errorf("could not store data: %w", err)
		log.Printf("%+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (srv *Server) handlePlotCO2(w http.ResponseWriter, r *http.Request) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	w.Header().Set("content-type", "image/png")
	w.Write(srv.plots.CO2.Bytes())
}

func (srv *Server) handlePlotH(w http.ResponseWriter, r *http.Request) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	w.Header().Set("content-type", "image/png")
	w.Write(srv.plots.H.Bytes())
}

func (srv *Server) handlePlotP(w http.ResponseWriter, r *http.Request) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	w.Header().Set("content-type", "image/png")
	w.Write(srv.plots.P.Bytes())
}

func (srv *Server) handlePlotT(w http.ResponseWriter, r *http.Request) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	w.Header().Set("content-type", "image/png")
	w.Write(srv.plots.T.Bytes())
}
