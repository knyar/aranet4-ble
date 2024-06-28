// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package arasrv // import "sbinet.org/x/aranet4/arasrv"

import (
	"fmt"
	"log"
	"sort"
	"time"

	"sbinet.org/x/aranet4"
)

func (srv *Server) init() error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	ids, err := srv.db.Devices()
	if err != nil {
		return fmt.Errorf("could not retrieve device ids: %w", err)
	}
	srv.ids = ids

	sort.Strings(srv.ids)
	for _, id := range srv.ids {
		srv.mgrs[id] = newManager(id)
	}

	var (
		beg = time.Unix(+0, 0)
		end = time.Unix(-1, 0)
	)
	for _, mgr := range srv.mgrs {
		last, err := srv.db.Last(mgr.id)
		if err != nil {
			return fmt.Errorf("could not find last data sample for %q: %w", mgr.id, err)
		}
		mgr.last = last

		data, err := mgr.rows(srv.db, beg, end)
		if err != nil {
			return fmt.Errorf("could not read data from db: %w", err)
		}

		err = mgr.plot(data)
		if err != nil {
			return fmt.Errorf("could not generate initial plots: %w", err)
		}
	}

	return nil
}

func (srv *Server) write(id string, vs []aranet4.Data) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	mgr, ok := srv.mgrs[id]
	if !ok {
		return fmt.Errorf("no such device %q", id)
	}

	if len(vs) == 0 {
		return nil
	}

	plural := ""
	if len(vs) > 1 {
		plural = "s"
	}
	log.Printf("writing %d new sample%s to db for device=%q...", len(vs), plural, id)

	err := srv.db.PutData(id, vs)
	if err != nil {
		return fmt.Errorf("could not write data slice to db: %w", err)
	}

	last, err := srv.db.Last(id)
	if err != nil {
		return fmt.Errorf("could not update last data for device %q: %w", id, err)
	}
	mgr.last = last

	return nil
}
