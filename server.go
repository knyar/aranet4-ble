// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4 // import "sbinet.org/x/aranet4"

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

type Server struct {
	addr string // Aranet4 device address
	mux  *http.ServeMux

	mu    sync.RWMutex
	db    *bbolt.DB
	last  Data
	plots struct {
		CO2     bytes.Buffer
		T, H, P bytes.Buffer
	}
}

func NewServer(addr, root, dbfile string) *Server {
	db, err := bbolt.Open(dbfile, 0644, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Panicf("could not open aranet4 db: %+v", err)
	}

	srv := &Server{
		addr: addr,
		db:   db,
		mux:  http.NewServeMux(),
	}

	root = strings.TrimRight(root, "/")
	srv.mux.HandleFunc(root+"/", srv.handleRoot)
	srv.mux.HandleFunc(root+"/favicon.ico", func(w http.ResponseWriter, r *http.Request) {})
	srv.mux.HandleFunc(root+"/update", srv.handleUpdate)
	srv.mux.HandleFunc(root+"/plot-co2", srv.handlePlotCO2)
	srv.mux.HandleFunc(root+"/plot-h", srv.handlePlotH)
	srv.mux.HandleFunc(root+"/plot-p", srv.handlePlotP)
	srv.mux.HandleFunc(root+"/plot-t", srv.handlePlotT)

	err = srv.init()
	if err != nil {
		log.Panicf("could not initialize server: %+v", err)
	}

	go srv.loop()
	return srv
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

func (srv *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	err := retry(10, func() error {
		return srv.update(-1)
	})
	if err != nil {
		fmt.Fprintf(w, "could not fetch update samples: %+v\n", err)
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

func (srv *Server) loop() {
	var (
		interval time.Duration
		err      error
	)
	err = retry(5, func() error {
		interval, err = srv.interval()
		return err
	})
	if err != nil {
		log.Panicf("could not fetch refresh frequency: %+v", err)
	}

	log.Printf("refresh frequency: %v", interval)
	tck := time.NewTicker(interval)
	defer tck.Stop()

	log.Printf("fetching history data...")
	err = retry(5, func() error {
		return srv.update(-1)
	})
	if err != nil {
		log.Printf("could not update db: %+v", err)
	}
	log.Printf("starting loop...")
	for range tck.C {
		log.Printf("tick: %s", time.Now().UTC().Format("2006-01-02 15:04:05"))
		err := retry(5, func() error {
			return srv.update(1)
		})
		if err != nil {
			log.Printf("could not update db: %+v", err)
		}
	}
}

func retry(n int, f func() error) error {
	var err error
	for i := 0; i < n; i++ {
		err = f()
		if err != nil {
			log.Printf("retry %d/%d failed with: %+v", i+1, n, err)
			continue
		}
		return nil
	}
	return err
}
