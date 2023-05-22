// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"tinygo.org/x/bluetooth"
)

var adapter = bluetooth.DefaultAdapter

const (
	defaultScanTimeout = 10 * time.Second
	defaultConnTimeout = 10 * time.Second
	defaultReadTimeout = 10 * time.Second
)

type Device struct {
	addr string
	name string
	dev  *bluetooth.Device
	svc  *bluetooth.DeviceService

	buf []byte
}

func New(ctx context.Context, addr string) (*Device, error) {
	err := adapter.Enable()
	if err != nil {
		return nil, fmt.Errorf("could not enable default adapter: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, defaultScanTimeout)
	defer cancel()

	var (
		scan    bluetooth.ScanResult
		errc    = make(chan error)
		errScan error
	)

	defer adapter.StopScan()
	go func() {
		errc <- adapter.Scan(func(adapter *bluetooth.Adapter, result bluetooth.ScanResult) {
			if result.Address.String() != addr {
				return
			}
			scan = result
			errScan = adapter.StopScan()
		})
	}()

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-errc:
	}
	if err != nil {
		return nil, fmt.Errorf("could not scan for %q: %w", addr, err)
	}
	if errScan != nil {
		return nil, fmt.Errorf("could not stop scan for %q: %w", addr, errScan)
	}

	dev, err := adapter.Connect(scan.Address, bluetooth.ConnectionParams{})
	if err != nil {
		return nil, fmt.Errorf("could not connect to %q: %w", addr, err)
	}

	svc, err := findService(dev, uuidDeviceService)
	if err != nil {
		_ = dev.Disconnect()
		return nil, fmt.Errorf("could not discover Aranet4 device service: %w", err)
	}

	return &Device{addr: addr, name: scan.LocalName(), dev: dev, svc: svc, buf: make([]byte, 256)}, nil
}

func (dev *Device) Close() error {
	if dev.dev == nil {
		return nil
	}

	err := dev.dev.Disconnect()
	if err != nil {
		return fmt.Errorf("could not disconnect: %w", err)
	}
	dev.dev = nil
	return nil
}

func (dev *Device) Name() string {
	return dev.name
}

func (dev *Device) Version() (string, error) {
	c, err := dev.devCharByUUID(uuidCommonReadSWRevision)
	if err != nil {
		return "", fmt.Errorf("could not get characteristic %q: %w", uuidCommonReadSWRevision, err)
	}

	raw, err := dev.read(c)
	if err != nil {
		return "", fmt.Errorf("could not read device name: %w", err)
	}
	return string(raw), nil
}

func (dev *Device) Read() (Data, error) {
	var data Data

	c, err := dev.devCharByUUID(uuidReadAll)
	if err != nil {
		return data, fmt.Errorf("could not get characteristic %q: %w", uuidReadAll, err)
	}

	raw, err := dev.read(c)
	if err != nil {
		return data, fmt.Errorf("could not get value: %w", err)
	}

	dec := newDecoder(bytes.NewReader(raw))
	dec.readCO2(&data.CO2)
	dec.readT(&data.T)
	dec.readP(&data.P)
	dec.readH(&data.H)
	dec.readBattery(&data.Battery)
	dec.readQuality(&data.Quality)
	dec.readInterval(&data.Interval)
	dec.readTime(&data.Time)

	if dec.err != nil {
		return data, fmt.Errorf("could not decode data sample: %w", dec.err)
	}

	return data, nil
}

func (dev *Device) NumData() (int, error) {
	c, err := dev.devCharByUUID(uuidReadTotalReadings)
	if err != nil {
		return 0, fmt.Errorf("could not get characteristic %q: %w", uuidReadTotalReadings, err)
	}

	raw, err := dev.read(c)
	if err != nil {
		return 0, fmt.Errorf("could not get value: %w", err)
	}

	return int(binary.LittleEndian.Uint16(raw)), nil
}

func (dev *Device) Since() (time.Duration, error) {
	c, err := dev.devCharByUUID(uuidReadSecondsSinceUpdate)
	if err != nil {
		return 0, fmt.Errorf("could not get characteristic %q: %w", uuidReadSecondsSinceUpdate, err)
	}

	raw, err := dev.read(c)
	if err != nil {
		return 0, fmt.Errorf("could not get value: %w", err)
	}

	var (
		ago time.Duration
		dec = newDecoder(bytes.NewReader(raw))
	)
	err = dec.readInterval(&ago)
	if err != nil {
		return 0, fmt.Errorf("could not decode interval value %q: %w", raw, err)
	}
	return ago, nil
}

func (dev *Device) Interval() (time.Duration, error) {
	c, err := dev.devCharByUUID(uuidReadInterval)
	if err != nil {
		return 0, fmt.Errorf("could not get characteristic %q: %w", uuidReadInterval, err)
	}

	raw, err := dev.read(c)
	if err != nil {
		return 0, fmt.Errorf("could not get value: %w", err)
	}

	var (
		ago time.Duration
		dec = newDecoder(bytes.NewReader(raw))
	)
	err = dec.readInterval(&ago)
	if err != nil {
		return 0, fmt.Errorf("could not decode interval value %q: %w", raw, err)
	}
	return ago, nil
}

func (dev *Device) ReadAll() ([]Data, error) {
	now := time.Now().UTC()
	ago, err := dev.Since()
	if err != nil {
		return nil, fmt.Errorf("could not get last measurement update: %w", err)
	}

	delta, err := dev.Interval()
	if err != nil {
		return nil, fmt.Errorf("could not get sampling: %w", err)
	}

	n, err := dev.NumData()
	if err != nil {
		return nil, fmt.Errorf("could not get total number of samples: %w", err)
	}
	out := make([]Data, n)
	for _, id := range []byte{paramT, paramH, paramP, paramCO2} {
		err = dev.readN(out, id)
		if err != nil {
			return nil, fmt.Errorf("could not read param=%d: %w", id, err)
		}
	}

	beg := now.Add(-ago - time.Duration(n-1)*delta)
	for i := range out {
		out[i].Battery = -1 // no battery information when fetching history.
		out[i].Quality = qualityFrom(out[i].CO2)
		out[i].Interval = delta
		out[i].Time = beg.Add(time.Duration(i) * delta)
	}

	return out, nil
}

func (dev *Device) read(c *bluetooth.DeviceCharacteristic) ([]byte, error) {
	n, err := c.Read(dev.buf)
	if err != nil {
		return nil, err
	}
	return dev.buf[:n], nil
}

func (dev *Device) readN(dst []Data, id byte) error {
	{
		cmd := []byte{
			0x82, 0x00, 0x00, 0x00, 0x01, 0x00, 0xff, 0xff,
		}
		cmd[1] = id
		binary.LittleEndian.PutUint16(cmd[4:], 0x0001)
		binary.LittleEndian.PutUint16(cmd[6:], 0xffff)

		c, err := dev.devCharByUUID(uuidWriteCmd)
		if err != nil {
			return fmt.Errorf("could not get characteristic %q: %w", uuidWriteCmd, err)
		}

		_, err = c.WriteWithoutResponse(cmd)
		if err != nil {
			return fmt.Errorf("could not write command: %w", err)
		}
	}

	c, err := dev.devCharByUUID(uuidReadTimeSeries)
	if err != nil {
		return fmt.Errorf("could not get characteristic %q: %w", uuidReadTimeSeries, err)
	}

	var (
		done        = make(chan struct{})
		ctx, cancel = context.WithTimeout(context.Background(), defaultReadTimeout)
		errc        = make(chan error)
	)
	defer cancel()

	go func() {
		errc <- c.EnableNotifications(func(p []byte) {
			param := p[0]
			if param != id {
				errc <- fmt.Errorf("invalid parameter: got=0x%x, want=0x%x", param, id)
				return
			}

			idx := int(binary.LittleEndian.Uint16(p[1:]) - 1)
			cnt := int(p[3])
			if cnt == 0 {
				close(done)
				return
			}
			max := min(idx+cnt, len(dst)) // a new sample may have appeared
			dec := newDecoder(bytes.NewReader(p[4:]))
			for i := idx; i < max; i++ {
				err := dec.readField(id, &dst[i])
				if err != nil {
					errc <- fmt.Errorf("could not read param=%d, idx=%d: %w", id, i, err)
					return
				}
			}
		})
	}()

	err = <-errc
	if err != nil {
		return fmt.Errorf("could not start notifications: %w", err)
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return fmt.Errorf("could not read notified data: %w", err)
	case <-done:
		return nil
	case err = <-errc:
		return fmt.Errorf("could not read notified data: %w", err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
