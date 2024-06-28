// Copyright ©2024 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package arabolt provides an implementation of an aranet4 database, backed by bbolt.
package arabolt // import "sbinet.org/x/aranet4/internal/arabolt"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"sort"
	"time"

	"go.etcd.io/bbolt"
	"sbinet.org/x/aranet4"
)

var (
	bucketRoot = []byte("aranet4")
	bucketIDs  = []byte("device-ids")
)

type DB struct {
	db *bbolt.DB

	last map[string]aranet4.Data
}

var _ aranet4.DB = (*DB)(nil)

// Open opens and initializes a boltdb-backed aranet4 database.
func Open(fname string) (*DB, error) {
	db, err := bbolt.Open(fname, 0644, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("could not open aranet4 db: %w", err)
	}

	var (
		devices []string
	)

	err = db.Update(func(tx *bbolt.Tx) error {
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
			devices = append(devices, id)
			return nil
		})
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("could not setup aranet4 db buckets: %w", err)
	}

	sort.Strings(devices)
	last := make(map[string]aranet4.Data, len(devices))

	for i := range devices {
		var (
			id   = devices[i]
			data aranet4.Data
		)
		err = db.View(func(tx *bbolt.Tx) error {
			root := tx.Bucket(bucketRoot)
			if root == nil {
				return fmt.Errorf("could not find %q bucket", bucketRoot)
			}

			bkt := root.Bucket([]byte(id))
			if bkt == nil {
				return fmt.Errorf("could not find data bucket for device %q", id)
			}

			return bkt.ForEach(func(k, v []byte) error {
				id := int64(binary.LittleEndian.Uint64(k))
				if id-data.Time.UTC().Unix() > timeResolution {
					return unmarshalBinary(&data, v)
				}
				return nil
			})
		})
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("could not find last data sample: %w", err)
		}
		last[id] = data
	}

	return &DB{db: db, last: last}, nil
}

// Close closes an aranet4 database
func (db *DB) Close() error {
	if db.db != nil {
		err := db.db.Close()
		if err != nil {
			return fmt.Errorf("could not close boltdb: %w", err)
		}
		db.db = nil
	}

	return nil
}

// PutData puts the provided data for the device id into the underlying store
func (db *DB) PutData(id string, vs []aranet4.Data) error {
	last, err := db.Last(id)
	if err != nil {
		switch {
		case errors.Is(err, aranet4.ErrNoData):
			// ok.
		default:
			return err
		}
	}

	sort.Slice(vs, func(i, j int) bool {
		return ltApprox(vs[i], vs[j])
	})

	idx := len(vs)
	for i, v := range vs {
		if ltApprox(last, v) {
			idx = i
			break
		}
	}
	vs = vs[idx:]
	if len(vs) == 0 {
		return nil
	}

	dataSize := aranet4.Data{}.BinarySize()
	deviceID := id
	err = db.db.Update(func(tx *bbolt.Tx) error {
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
			if ltApprox(last, v) {
				v.Quality = aranet4.QualityFrom(v.CO2)
				db.last[deviceID] = v
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not write data slice to db: %w", err)
	}
	return nil
}

// Data iterates over data for the device id and the requested time interval [beg, end)
func (db *DB) Data(id string, beg, end time.Time) iter.Seq2[aranet4.Data, error] {
	return func(yield func(data aranet4.Data, err error) bool) {
		var (
			beg  = beg.UTC().Unix()
			end  = end.UTC().Unix()
			rows []aranet4.Data
		)
		err := db.db.View(func(tx *bbolt.Tx) error {
			root := tx.Bucket(bucketRoot)
			if root == nil {
				return fmt.Errorf("could not find %q bucket", bucketRoot)
			}

			bkt := root.Bucket([]byte(id))
			if bkt == nil {
				return fmt.Errorf("could not find data bucket for device=%q", id)
			}

			return bkt.ForEach(func(k, v []byte) error {
				var (
					row aranet4.Data
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
			_ = yield(aranet4.Data{}, fmt.Errorf("could not read rows: %w", err))
			return
		}

		sort.Slice(rows, func(i, j int) bool {
			return ltApprox(rows[i], rows[j])
		})

		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

// Last returns the last data point for the provided device id
func (db *DB) Last(id string) (aranet4.Data, error) {
	last, ok := db.last[id]
	if !ok {
		return last, fmt.Errorf("no such device %q", id)
	}

	if last.Time.IsZero() {
		return last, aranet4.ErrNoData
	}

	return last, nil
}

// AddDevice declares a new device id
func (db *DB) AddDevice(id string) error {
	err := db.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket(bucketRoot)
		if root == nil {
			return fmt.Errorf("could not access %q bucket", bucketRoot)
		}

		ids := root.Bucket(bucketIDs)
		if ids == nil {
			return fmt.Errorf("could not access %q bucket", bucketIDs)
		}
		err := ids.Put([]byte(id), []byte(id))
		if err != nil {
			return fmt.Errorf("could not store device id %q: %w", id, err)
		}

		_, err = root.CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return fmt.Errorf("could not create data bucket for device %q: %w", id, err)
		}
		return nil

	})
	if err != nil {
		return fmt.Errorf("could not add device %q: %w", id, err)
	}
	db.last[id] = aranet4.Data{}
	return nil
}

// Devices returns the device ids list
func (db *DB) Devices() ([]string, error) {
	devices := make([]string, 0, len(db.last))
	for id := range db.last {
		devices = append(devices, id)
	}
	sort.Strings(devices)
	return devices, nil
}

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

func unmarshalBinary(data *aranet4.Data, p []byte) error {
	return data.Unmarshal(p)
}

func marshalBinary(data aranet4.Data, p []byte) error {
	return data.Marshal(p)
}
