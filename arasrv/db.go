// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package arasrv // import "sbinet.org/x/aranet4/arasrv"

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	"go.etcd.io/bbolt"
	"sbinet.org/x/aranet4"
)

const (
	timeResolution int64 = 5 // seconds
)

func ltApprox(a, b aranet4.Data) bool {
	at := a.Time.UTC().Unix()
	bt := b.Time.UTC().Unix()
	if abs(at-bt) < timeResolution {
		return false
	}
	return at < bt
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

var (
	bucketRoot = []byte("aranet4")
	bucketIDs  = []byte("device-ids")
)

func (srv *Server) init() error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	err := srv.db.Update(func(tx *bbolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists(bucketRoot)
		if err != nil {
			return fmt.Errorf("could not create %q bucket: %w", bucketRoot, err)
		}
		if root == nil {
			return fmt.Errorf("could not create %q bucket", bucketRoot)
		}

		ids, err := root.CreateBucketIfNotExists(bucketIDs)
		if err != nil {
			return fmt.Errorf("could not create %q bucket: %w", bucketIDs, err)
		}
		if ids == nil {
			return fmt.Errorf("could not create %q bucket", bucketIDs)
		}

		return ids.ForEach(func(k, v []byte) error {
			id := string(k)
			srv.ids = append(srv.ids, id)
			srv.mgrs[id] = newManager(id)
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("could not setup aranet4 db buckets: %w", err)
	}
	sort.Strings(srv.ids)

	for _, mgr := range srv.mgrs {
		err = srv.db.View(func(tx *bbolt.Tx) error {
			root := tx.Bucket(bucketRoot)
			if root == nil {
				return fmt.Errorf("could not find %q bucket", bucketRoot)
			}

			bkt := root.Bucket([]byte(mgr.id))
			if bkt == nil {
				return fmt.Errorf("could not find data bucket for device %q", mgr.id)
			}

			return bkt.ForEach(func(k, v []byte) error {
				id := int64(binary.LittleEndian.Uint64(k))
				if id-mgr.last.Time.UTC().Unix() > timeResolution {
					return unmarshalBinary(&mgr.last, v)
				}
				return nil
			})
		})
		if err != nil {
			return fmt.Errorf("could not find last data sample: %w", err)
		}

		var (
			beg int64 = 0
			end int64 = -1
		)
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

	if len(vs) == 0 {
		return nil
	}

	sort.Slice(vs, func(i, j int) bool {
		return ltApprox(vs[i], vs[j])
	})

	mgr, ok := srv.mgrs[id]
	if mgr == nil || !ok {
		return fmt.Errorf("could not find device manager for id=%q", id)
	}

	// consolidate data-from-sensor and time-series from db.
	idx := len(vs)
	for i, v := range vs {
		if ltApprox(mgr.last, v) {
			idx = i
			break
		}
	}
	vs = vs[idx:]
	if len(vs) == 0 {
		return nil
	}

	plural := ""
	if len(vs) > 1 {
		plural = "s"
	}
	log.Printf("writing %d new sample%s to db for device=%q...", len(vs), plural, id)
	err := srv.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket(bucketRoot)
		if root == nil {
			return fmt.Errorf("could not access %q bucket", bucketRoot)
		}

		bkt := root.Bucket([]byte(id))
		if bkt == nil {
			return fmt.Errorf("could not access data bucket for device %q", id)
		}

		for _, v := range vs {
			var (
				id  = make([]byte, 8)
				buf = make([]byte, dataSize)
			)
			unix := v.Time.UTC().Unix()
			binary.LittleEndian.PutUint64(id, uint64(unix))
			err := marshalBinary(v, buf)
			if err != nil {
				return fmt.Errorf("could not marshal sample %v: %w", v, err)
			}

			err = bkt.Put(id, buf)
			if err != nil {
				return fmt.Errorf("could not store sample %v: %w", v, err)
			}
			if ltApprox(mgr.last, v) {
				mgr.last = v
				mgr.last.Quality = aranet4.QualityFrom(mgr.last.CO2)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("could not write data slice to db: %w", err)
	}
	return nil
}

const dataSize = 17

func unmarshalBinary(data *aranet4.Data, p []byte) error {
	if len(p) != dataSize {
		return io.ErrShortBuffer
	}
	data.Time = time.Unix(int64(binary.LittleEndian.Uint64(p)), 0).UTC()
	data.H = float64(p[8])
	data.P = float64(binary.LittleEndian.Uint16(p[9:])) / 10
	data.T = float64(binary.LittleEndian.Uint16(p[11:])) / 100
	data.CO2 = int(binary.LittleEndian.Uint16(p[13:]))
	data.Battery = int(p[15])
	data.Quality = aranet4.QualityFrom(data.CO2)
	data.Interval = time.Duration(p[16]) * time.Minute
	return nil
}

func marshalBinary(data aranet4.Data, p []byte) error {
	if len(p) != dataSize {
		return io.ErrShortBuffer
	}
	binary.LittleEndian.PutUint64(p[0:], uint64(data.Time.UTC().Unix()))
	p[8] = uint8(data.H)
	binary.LittleEndian.PutUint16(p[9:], uint16(data.P*10))
	binary.LittleEndian.PutUint16(p[11:], uint16(data.T*100))
	binary.LittleEndian.PutUint16(p[13:], uint16(data.CO2))
	p[15] = uint8(data.Battery)
	p[16] = uint8(data.Interval.Minutes())
	return nil
}
