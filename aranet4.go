// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4 // import "sbinet.org/x/aranet4"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	timeFmt = "2006-01-02 15:04:05 UTC"
)

var (
	uuidDeviceService          = "f0cd1400-95da-4f4b-9ac8-aa55d312af0c"
	uuidDeviceServiceV1_2_0    = "0000fce0-0000-1000-8000-00805f9b34fb" // for firmware v1.2.0 and later
	uuidWriteCmd               = "f0cd1402-95da-4f4b-9ac8-aa55d312af0c"
	uuidReadSample             = "f0cd1503-95da-4f4b-9ac8-aa55d312af0c"
	uuidReadAll                = "f0cd3001-95da-4f4b-9ac8-aa55d312af0c"
	uuidReadInterval           = "f0cd2002-95da-4f4b-9ac8-aa55d312af0c"
	uuidReadTimeSeries         = "f0cd2003-95da-4f4b-9ac8-aa55d312af0c"
	uuidReadSecondsSinceUpdate = "f0cd2004-95da-4f4b-9ac8-aa55d312af0c"
	uuidReadTotalReadings      = "f0cd2001-95da-4f4b-9ac8-aa55d312af0c"

	uuidGenericService = "00001800-0000-1000-8000-00805f9b34fb"

	uuidCommonService               = "0000180a-0000-1000-8000-00805f9b34fb"
	uuidCommonReadManufacturerName  = "00002a29-0000-1000-8000-00805f9b34fb"
	uuidCommonReadModelNumber       = "00002a24-0000-1000-8000-00805f9b34fb"
	uuidCommonReadSerialNumber      = "00002a25-0000-1000-8000-00805f9b34fb"
	uuidCommonReadSWRevision        = "00002a26-0000-1000-8000-00805f9b34fb"
	uuidCommonReadHWRevision        = "00002a27-0000-1000-8000-00805f9b34fb"
	uuidCommonReadFactorySWRevision = "00002a28-0000-1000-8000-00805f9b34fb"
	uuidCommonReadBattery           = "00002a19-0000-1000-8000-00805f9b34fb"
)

const (
	paramT   = 1
	paramH   = 2
	paramP   = 3
	paramCO2 = 4
)

var (
	// ErrNoData indicates a missing data point.
	// This may happen during sensor calibration.
	ErrNoData = errors.New("aranet4: no data")

	// ErrDupDevice is returned by DB.AddDevice when a device with
	// the provided id is already stored in the database.
	ErrDupDevice = errors.New("aranet4: duplicate device")

	// errNoSvc indicates to service could be found for a given device.
	errNoSvc = errors.New("aranet4: no service attached to device")
)

// Quality gives a general assessment of air quality (green/yellow/red).
//   - green:  [   0 - 1000) ppm
//   - yellow: [1000 - 1400) ppm
//   - red:    [1400 -  ...) ppm
type Quality int

func (st Quality) String() string {
	switch st {
	case 1:
		return "green"
	case 2:
		return "yellow"
	case 3:
		return "red"
	default:
		return fmt.Sprintf("Quality(%d)", int(st))
	}
}

// QualityFrom creates a quality value from a CO2 value.
func QualityFrom(co2 int) Quality {
	switch {
	case co2 < 1000:
		return 1
	case co2 < 1400:
		return 2
	default:
		return 3
	}
}

// Data holds measured data samples provided by Aranet4.
type Data struct {
	H, P, T float64
	CO2     int
	Battery int
	Quality Quality

	Interval time.Duration
	Time     time.Time
}

// keep dataSize synchronized with Data.
const dataSize = 17

// BinarySize returns the number of bytes needed to hold the binary data
// for a single Data element.
func (Data) BinarySize() int {
	return dataSize
}

func (data Data) String() string {
	var o strings.Builder
	fmt.Fprintf(&o, "CO2:         %d ppm\n", data.CO2)
	fmt.Fprintf(&o, "temperature: %g°C\n", data.T)
	fmt.Fprintf(&o, "pressure:    %g hPa\n", data.P)
	fmt.Fprintf(&o, "humidity:    %g%%\n", data.H)
	fmt.Fprintf(&o, "quality:     %v\n", data.Quality)
	fmt.Fprintf(&o, "battery:     %d%%\n", data.Battery)
	fmt.Fprintf(&o, "interval:    %v\n", data.Interval)
	fmt.Fprintf(&o, "time-stamp:  %v\n", data.Time.UTC().Format(timeFmt))
	return o.String()
}

func (data *Data) Unmarshal(p []byte) error {
	if len(p) != dataSize {
		return io.ErrShortBuffer
	}
	data.Time = time.Unix(int64(binary.LittleEndian.Uint64(p)), 0).UTC()
	data.H = float64(p[8])
	data.P = float64(binary.LittleEndian.Uint16(p[9:])) / 10
	data.T = float64(binary.LittleEndian.Uint16(p[11:])) / 100
	data.CO2 = int(binary.LittleEndian.Uint16(p[13:]))
	data.Battery = int(p[15])
	data.Quality = QualityFrom(data.CO2)
	data.Interval = time.Duration(p[16]) * time.Minute
	return nil
}

func (data Data) Marshal(p []byte) error {
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

func (data Data) Before(o Data) bool {
	return ltApprox(data, o)
}

// Samples implements sort.Interface for Data, sorting in increasing timestamps.
type Samples []Data

func (vs Samples) Len() int           { return len(vs) }
func (vs Samples) Swap(i, j int)      { vs[i], vs[j] = vs[j], vs[i] }
func (vs Samples) Less(i, j int) bool { return ltApprox(vs[i], vs[j]) }

const (
	timeResolution int64 = 5 // seconds
)

func ltApprox(a, b Data) bool {
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
