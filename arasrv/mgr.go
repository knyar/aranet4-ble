// Copyright ©2023 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package arasrv // import "sbinet.org/x/aranet4/arasrv"

import (
	"bytes"
	"fmt"
	"time"

	"sbinet.org/x/aranet4"
)

type manager struct {
	id string

	last  aranet4.Data
	plots struct {
		CO2     bytes.Buffer
		T, H, P bytes.Buffer
	}
}

func newManager(id string) *manager {
	return &manager{id: id}
}

func (mgr *manager) rows(db aranet4.DB, beg, end time.Time) (rows []aranet4.Data, err error) {
	for row, err := range db.Data(mgr.id, beg, end) {
		if err != nil {
			return nil, fmt.Errorf("could not read rows: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}
