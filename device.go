// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rigado/ble"
)

type Device struct {
	addr    string
	name    string
	dev     ble.Client
	profile *ble.Profile
}

func New(ctx context.Context, addr string) (*Device, error) {
	const scanDeadline = 15 * time.Second
	ctx = ble.WithSigHandler(context.WithTimeout(ctx, scanDeadline))

	cln, err := ble.Connect(ctx, func(a ble.Advertisement) bool {
		return strings.EqualFold(a.Addr().String(), addr)
	})
	if err != nil {
		return nil, fmt.Errorf("could not connect to device %q: %w", addr, err)
	}

	log.Printf("connected to device %q", addr)

	name := cln.Name()

	profile, err := cln.DiscoverProfile(true)
	if err != nil {
		_ = cln.CancelConnection()
		return nil, fmt.Errorf("could not discover profile: %w", err)
	}
	return &Device{
		addr:    addr,
		name:    name,
		dev:     cln,
		profile: profile,
	}, nil
}

func (dev *Device) Client() ble.Client {
	return dev.dev
}

func (dev *Device) Close() error {
	if dev.dev == nil {
		return nil
	}
	defer func() {
		<-dev.dev.Disconnected()
		log.Printf("disconnected from device %q", dev.addr)
		dev.dev = nil
	}()

	err := dev.dev.CancelConnection()
	if err != nil {
		return fmt.Errorf("could not disconnect: %w", err)
	}
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
		out[i].Quality = QualityFrom(out[i].CO2)
		out[i].Interval = delta
		out[i].Time = beg.Add(time.Duration(i) * delta)
	}

	return out, nil
}

func (dev *Device) read(c *ble.Characteristic) ([]byte, error) {
	b, err := dev.dev.ReadCharacteristic(c)
	return b, err
}

func (dev *Device) readN(dst []Data, id byte) error {
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

	err = dev.dev.WriteCharacteristic(c, cmd, false)
	if err != nil {
		return fmt.Errorf("could not write command: %w", err)
	}

	c, err = dev.devCharByUUID(uuidReadTimeSeries)
	if err != nil {
		return fmt.Errorf("could not get characteristic %q: %w", uuidReadTimeSeries, err)
	}

	errs := make(chan error)
	handler := func(_ uint, b []byte) {
		err := func(p []byte) error {
			param := p[0]
			if param != id {
				return fmt.Errorf("invalid parameter: got=0x%x, want=0x%x", param, id)
			}

			idx := int(binary.LittleEndian.Uint16(p[1:]) - 1)
			cnt := int(p[3])
			if cnt == 0 {
				close(errs)
				return nil
			}
			max := min(idx+cnt, len(dst)) // a new sample may have appeared
			dec := newDecoder(bytes.NewReader(p[4:]))
			for i := idx; i < max; i++ {
				err := dec.readField(id, &dst[i])
				if err != nil {
					if !errors.Is(err, ErrNoData) {
						return fmt.Errorf("could not read param=%d, idx=%d: %w", id, i, err)
					}
					log.Printf("could not read param=%d, idx=%d: %+v", id, i, err)
				}
			}
			return nil
		}(b)
		if err != nil {
			errs <- err
		}
	}

	if err := dev.dev.Subscribe(c, false, handler); err != nil {
		return fmt.Errorf("could not subscribe to characteristic %q: %w", uuidReadTimeSeries, err)
	}
	defer func() {
		if err := dev.dev.Unsubscribe(c, false); err != nil {
			log.Printf("could not unsubscribe from characteristic %q: %v", uuidReadTimeSeries, err)
		}
	}()

	if err, ok := <-errs; ok {
		return fmt.Errorf("could not read notified data: %w", err)
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
