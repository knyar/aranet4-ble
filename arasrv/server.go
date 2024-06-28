// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package arasrv // import "sbinet.org/x/aranet4/arasrv"

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"sbinet.org/x/aranet4"
)

type Server struct {
	mux *http.ServeMux

	mu   sync.RWMutex
	db   *bbolt.DB
	ids  []string
	mgrs map[string]*manager

	root string
	tmpl *template.Template
}

func NewServer(root, dbfile string) (*Server, error) {
	db, err := bbolt.Open(dbfile, 0644, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("could not open aranet4 db: %w", err)
	}

	srv := &Server{
		db:   db,
		mux:  http.NewServeMux(),
		mgrs: make(map[string]*manager),
		root: root,
		tmpl: template.Must(template.New("aranet4").Parse(page)),
	}

	root = strings.TrimRight(root, "/")
	srv.mux.HandleFunc(root+"/", srv.handleRoot)
	srv.mux.HandleFunc(root+"/favicon.ico", func(w http.ResponseWriter, r *http.Request) {})
	srv.mux.HandleFunc(root+"/post", srv.handleIngest)
	srv.mux.HandleFunc(root+"/plot-co2", srv.handlePlotCO2)
	srv.mux.HandleFunc(root+"/plot-h", srv.handlePlotH)
	srv.mux.HandleFunc(root+"/plot-p", srv.handlePlotP)
	srv.mux.HandleFunc(root+"/plot-t", srv.handlePlotT)
	srv.mux.HandleFunc(root+"/api", srv.handleAPI)

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

	mgr, err := srv.mgrFor(r)
	if err != nil {
		err = fmt.Errorf("could not find device manager: %w", err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
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

	data, err := mgr.rows(srv.db, beg, end)
	if err != nil {
		err = fmt.Errorf("could not read rows for device=%q from db: %w", mgr.id, err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = mgr.plot(data)
	if err != nil {
		err = fmt.Errorf("could not create plots for device=%q: %w", mgr.id, err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	refresh := int(mgr.last.Interval.Seconds())
	if refresh == 0 {
		refresh = 10
	}
	ctx := struct {
		Root     string
		Devices  []string
		DeviceID string
		Status   string
		Refresh  int
		From     string
		To       string
	}{
		Root:     srv.root,
		Devices:  srv.ids,
		DeviceID: mgr.id,
		Status:   mgr.last.String(),
		Refresh:  refresh,
	}

	if beg > 0 {
		ctx.From = time.Unix(beg, 0).UTC().Format("2006-01-02")
	}
	if end > 0 {
		ctx.To = time.Unix(end, 0).UTC().Format("2006-01-02")
	}

	err = srv.tmpl.Execute(w, ctx)
	if err != nil {
		err = fmt.Errorf("could not display page for device=%q: %w", mgr.id, err)
		fmt.Fprintf(w, "%+v", err)
		log.Printf("error: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (srv *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		err := fmt.Errorf("invalid HTTP method: %s", r.Method)
		log.Printf("%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var (
		req struct {
			ID   string         `json:"device_id"`
			Data []aranet4.Data `json:"data"`
		}
		err = json.NewDecoder(r.Body).Decode(&req)
	)
	if err != nil {
		err = fmt.Errorf("could not decode JSON payload: %w", err)
		log.Printf("%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mgr, ok := srv.mgrs[req.ID]
	if mgr == nil || !ok {
		err := fmt.Errorf("could not find device manager for device=%q", req.ID)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = srv.write(mgr.id, req.Data)
	if err != nil {
		err = fmt.Errorf("could not store data for device=%q: %w", req.ID, err)
		log.Printf("%+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (srv *Server) handlePlotCO2(w http.ResponseWriter, r *http.Request) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	mgr, err := srv.mgrFor(r)
	if err != nil {
		err = fmt.Errorf("could not find device manager: %w", err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "image/png")
	w.Write(mgr.plots.CO2.Bytes())
}

func (srv *Server) handlePlotH(w http.ResponseWriter, r *http.Request) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	mgr, err := srv.mgrFor(r)
	if err != nil {
		err = fmt.Errorf("could not find device manager: %w", err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "image/png")
	w.Write(mgr.plots.H.Bytes())
}

func (srv *Server) handlePlotP(w http.ResponseWriter, r *http.Request) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	mgr, err := srv.mgrFor(r)
	if err != nil {
		err = fmt.Errorf("could not find device manager: %w", err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "image/png")
	w.Write(mgr.plots.P.Bytes())
}

func (srv *Server) handlePlotT(w http.ResponseWriter, r *http.Request) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	mgr, err := srv.mgrFor(r)
	if err != nil {
		err = fmt.Errorf("could not find device manager: %w", err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "image/png")
	w.Write(mgr.plots.T.Bytes())
}

func (srv *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		fmt.Fprintf(w, "could not parse form: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	mgr, err := srv.mgrFor(r)
	if err != nil {
		err = fmt.Errorf("could not find device manager: %w", err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
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

	data, err := mgr.rows(srv.db, beg, end)
	if err != nil {
		err = fmt.Errorf("could not read rows for device=%q from db: %w", mgr.id, err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = mgr.plot(data)
	if err != nil {
		err = fmt.Errorf("could not create plots for device=%q: %w", mgr.id, err)
		fmt.Fprintf(w, "%+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	refresh := int(mgr.last.Interval.Seconds())
	if refresh == 0 {
		refresh = 10
	}

	msg := Message{
		Root:     srv.root,
		Devices:  srv.ids,
		DeviceID: mgr.id,
		Status:   mgr.last.String(),
		Refresh:  refresh,
	}

	if beg > 0 {
		msg.From = time.Unix(beg, 0).UTC().Format("2006-01-02")
	}
	if end > 0 {
		msg.To = time.Unix(end, 0).UTC().Format("2006-01-02")
	}
	msg.Plots.CO2 = base64.StdEncoding.EncodeToString(mgr.plots.CO2.Bytes())
	msg.Plots.H = base64.StdEncoding.EncodeToString(mgr.plots.H.Bytes())
	msg.Plots.P = base64.StdEncoding.EncodeToString(mgr.plots.P.Bytes())
	msg.Plots.T = base64.StdEncoding.EncodeToString(mgr.plots.T.Bytes())

	buf := new(bytes.Buffer)
	err = json.NewEncoder(buf).Encode(msg)
	if err != nil {
		err = fmt.Errorf("could not encode message for device=%q: %w", mgr.id, err)
		fmt.Fprintf(w, "%+v", err)
		log.Printf("error: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = io.Copy(w, buf)
	if err != nil {
		err = fmt.Errorf("could not write message for device=%q: %w", mgr.id, err)
		fmt.Fprintf(w, "%+v", err)
		log.Printf("error: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (srv *Server) mgrFor(r *http.Request) (*manager, error) {
	id := r.Form.Get("device_id")
	if id == "" {
		if len(srv.mgrs) > 1 {
			return nil, fmt.Errorf("could not find device_id parameter form")
		}
		id = srv.ids[0]
	}

	mgr, ok := srv.mgrs[id]
	if !ok {
		return nil, fmt.Errorf("could not find manager for device=%q", id)
	}

	return mgr, nil
}
