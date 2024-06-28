// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4 // import "sbinet.org/x/aranet4"

import (
	"time"
)

type DB interface {
	Init() error
	AddData(vs []Data) error
	Data(beg, end time.Time) ([]Data, error)

	AddDevice() error
	Devices() ([]string, error)
}
