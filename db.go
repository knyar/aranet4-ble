// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4 // import "sbinet.org/x/aranet4"

import (
	"io"
	"iter"
	"time"
)

// DB is the interface to manage aranet4 data.
type DB interface {
	io.Closer

	// PutData puts the provided data for the device id into the underlying store
	PutData(id string, vs []Data) error

	// Data iterates over data for the device id and the requested time interval [beg, end)
	Data(id string, beg, end time.Time) iter.Seq2[Data, error]

	// Last returns the last data point for the provided device id
	Last(id string) (Data, error)

	// AddDevice declares a new device id
	AddDevice(id string) error

	// Devices returns the device ids list
	Devices() ([]string, error)
}
