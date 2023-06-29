// Copyright ©2023 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4 // import "sbinet.org/x/aranet4"

import (
	"bytes"
	"fmt"
	"sort"

	"go.etcd.io/bbolt"
)

type manager struct {
	id string

	last  Data
	plots struct {
		CO2     bytes.Buffer
		T, H, P bytes.Buffer
	}
}

func newManager(id string) *manager {
	return &manager{id: id}
}

func (mgr *manager) rows(db *bbolt.DB, beg, end int64) ([]Data, error) {
	var rows []Data
	err := db.View(func(tx *bbolt.Tx) error {
		root := tx.Bucket(bucketRoot)
		if root == nil {
			return fmt.Errorf("could not find %q bucket", bucketRoot)
		}

		bkt := root.Bucket([]byte(mgr.id))
		if bkt == nil {
			return fmt.Errorf("could not find data bucket for device=%q", mgr.id)
		}

		return bkt.ForEach(func(k, v []byte) error {
			var (
				row Data
				err = unmarshalBinary(&row, v)
			)
			if err != nil {
				return err
			}
			id := row.Time.UTC().Unix()
			if beg > id {
				return nil
			}
			if end > 0 && id > end {
				return nil
			}
			rows = append(rows, row)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("could not read rows: %w", err)
	}

	sort.Slice(rows, func(i, j int) bool {
		return ltApprox(rows[i], rows[j])
	})

	return rows, nil
}
