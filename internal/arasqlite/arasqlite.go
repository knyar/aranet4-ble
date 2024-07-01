// Copyright ©2024 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package arasqlite provides an implementation of an aranet4 database, backed by SQlite3.
package arasqlite // import "sbinet.org/x/aranet4/internal/arasqlite"

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
	"sbinet.org/x/aranet4"
)

type DB struct {
	db *sql.DB

	last map[string]aranet4.Data
}

var _ aranet4.DB = (*DB)(nil)

// Open opens and initializes a sqlite3-backed aranet4 database.
func Open(fname string) (*DB, error) {
	if _, err := os.Stat(fname); errors.Is(err, fs.ErrNotExist) {
		err = createDB(context.Background(), fname)
		if err != nil {
			return nil, fmt.Errorf("could not create aranet4 db: %w", err)
		}
	}

	db, err := sql.Open("sqlite", fname)
	if err != nil {
		return nil, fmt.Errorf("could not open aranet4 db %q: %w", fname, err)
	}

	store := &DB{
		db:   db,
		last: make(map[string]aranet4.Data),
	}
	err = store.init()
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("could not setup aranet4 db %q: %w", fname, err)
	}

	return store, nil
}

func createDB(ctx context.Context, fname string) error {
	f, err := os.Create(fname)
	if err != nil {
		return fmt.Errorf("could not create aranet4 db %q: %w", fname, err)
	}
	defer f.Close()

	db, err := sql.Open("sqlite", fname)
	if err != nil {
		return fmt.Errorf("could not open aranet4 db %q: %w", fname, err)
	}
	defer db.Close()

	{
		stmt := `CREATE TABLE devices (
        id    TEXT NOT NULL PRIMARY KEY, -- device id (bluetooth id)
		name  TEXT NOT NULL              -- table name for this device
)
`
		_, err = db.ExecContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf("could not create devices table %q: %w", fname, err)
		}
	}

	// Use Write Ahead Logging which improves SQLite concurrency.
	// Requires SQLite >= 3.7.0
	_, err = db.ExecContext(ctx, "PRAGMA journal_mode = WAL")
	if err != nil {
		return fmt.Errorf("could not set WAL mode: %w", err)
	}

	// Check if the WAL mode was set correctly
	var journalMode string
	if err = db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		return fmt.Errorf("could not determine sqlite3 journal_mode: %w", err)
	}
	if journalMode != "wal" {
		return fmt.Errorf("could not set sqlite WAL mode")
	}

	return nil
}

func (db *DB) init() error {
	{
		const stmt = `SELECT id FROM devices`
		rows, err := db.db.Query(stmt)
		if err != nil {
			return fmt.Errorf("could not retrieve devices list: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var (
				id  string
				err = rows.Scan(&id)
			)
			if err != nil {
				return fmt.Errorf("could not scan device id row: %w", err)
			}
			db.last[id] = aranet4.Data{}
		}
	}
	{
		ids := make([]string, 0, len(db.last))
		for id := range db.last {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for i := range ids {
			err := func(id string) error {
				tbl := db.table(id)
				rows, err := db.db.Query(`SELECT time, h, p, t, co2, battery, interval FROM ` + tbl + ` ORDER BY time DESC`)
				if err != nil {
					return fmt.Errorf("could not issue query: %w", err)
				}
				defer rows.Close()

				if !rows.Next() {
					// no data.
					return nil
				}

				var (
					row  aranet4.Data
					ts   int64
					freq int
				)
				err = rows.Scan(&ts, &row.H, &row.P, &row.T, &row.CO2, &row.Battery, &freq)
				if err != nil {
					return fmt.Errorf("could not scan row: %w", err)
				}
				row.Time = time.Unix(ts, 0).UTC()
				row.Interval = time.Duration(freq) * time.Minute
				row.Quality = aranet4.QualityFrom(row.CO2)
				db.last[id] = row
				return nil
			}(ids[i])
			if err != nil {
				return fmt.Errorf("could not fetch last data point for device %q: %w", ids[i], err)
			}
		}
	}

	return nil
}

func (db *DB) table(id string) string {
	sha := sha256.New224()
	_, err := io.Copy(sha, strings.NewReader(id))
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("dev_%x", sha.Sum(nil))
}

// Close closes an aranet4 database
func (db *DB) Close() error {
	if db.db != nil {
		err := db.db.Close()
		if err != nil {
			return fmt.Errorf("could not close sqlite db: %w", err)
		}
		db.db = nil
	}

	return nil
}

// PutData puts the provided data for the device id into the underlying store
func (db *DB) PutData(id string, vs []aranet4.Data) (err error) {
	last, err := db.Last(id)
	if err != nil {
		switch {
		case errors.Is(err, aranet4.ErrNoData):
			// ok.
		default:
			return err
		}
	}

	tbl := db.table(id)
	sort.Sort(aranet4.Samples(vs))

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("could not create sqlite transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	stmt := `INSERT INTO ` + tbl + ` (
	time,
	h,
	p,
	t,
	co2,
	battery,
	interval
) VALUES
(?1, ?2, ?3, ?4, ?5, ?6, ?7)
`
	for _, v := range vs {
		_, err = tx.Exec(stmt,
			v.Time.Unix(),
			v.H,
			v.P,
			v.T,
			v.CO2,
			v.Battery,
			int(v.Interval.Minutes()),
		)
		if err != nil {
			return fmt.Errorf("could not insert data %q: %w", v.Time, err)
		}
		if last.Before(v) {
			v.Quality = aranet4.QualityFrom(v.CO2)
			db.last[id] = v
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("could not commit sqlite transaction: %w", err)
	}

	return nil
}

// Data iterates over data for the device id and the requested time interval [beg, end)
func (db *DB) Data(id string, beg, end time.Time) iter.Seq2[aranet4.Data, error] {
	return func(yield func(data aranet4.Data, err error) bool) {
		var (
			tbl = db.table(id)
			beg = beg.UTC().Unix()
			end = end.UTC().Unix()
		)
		q := "SELECT time, h, p, t, co2, battery, interval FROM " + tbl
		if beg > 0 || end > 0 {
			q += " WHERE\n"
			if beg > 0 {
				q += " ?1 <= time"
			}
			if end > 0 {
				if beg > 0 {
					q += " AND"
				}
				q += " time < ?2"
			}
		}
		q += " ORDER BY time ASC"

		rows, err := db.db.Query(q, beg, end)
		if err != nil {
			_ = yield(aranet4.Data{}, fmt.Errorf("could not issue query: %w", err))
			return
		}
		defer rows.Close()

		for i := 0; rows.Next(); i++ {
			var (
				row  aranet4.Data
				ts   int64
				freq int
			)
			err = rows.Scan(&ts, &row.H, &row.P, &row.T, &row.CO2, &row.Battery, &freq)
			if err != nil {
				_ = yield(row, fmt.Errorf("could not scan row %d: %w", i, err))
				return
			}
			row.Time = time.Unix(ts, 0).UTC()
			row.Interval = time.Duration(freq) * time.Minute
			row.Quality = aranet4.QualityFrom(row.CO2)
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
func (db *DB) AddDevice(id string) (err error) {
	if _, dup := db.last[id]; dup {
		return aranet4.ErrDupDevice
	}

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("could not create sqlite transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	name := db.table(id)
	{
		const q = `INSERT INTO devices (id, name) VALUES (?1, ?2)`
		_, err = tx.Exec(q, id, name)
		if err != nil {
			return fmt.Errorf("could not add device %q to devices table: %w", id, err)
		}
	}
	{
		stmt := `CREATE TABLE ` + name + ` (
			time    INTEGER NOT NULL PRIMARY KEY, -- timestamp (seconds since epoch UTC)
			h        DOUBLE,                      -- humidity (in %)
			p        DOUBLE,                      -- pressure (in hPa)
			t        DOUBLE,                      -- temperature (in °C)
			co2      INTEGER,                     -- CO2 level (in ppm)
			battery  INTEGER,                     -- battery level (in %)
			interval INTEGER                      -- sensor refresh interval (in minutes)
)
`
		_, err = tx.Exec(stmt)
		if err != nil {
			return fmt.Errorf("could not create device table for %q: %w", id, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("could not commit sqlite transaction for device %q: %w", id, err)
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
