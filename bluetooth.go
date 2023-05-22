// Copyright ©2023 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4

import (
	"fmt"

	"tinygo.org/x/bluetooth"
)

func mustParseUUID(v string) bluetooth.UUID {
	o, err := bluetooth.ParseUUID(v)
	if err != nil {
		panic(fmt.Errorf("aranet4: could not parse UUID %q: %+v", v, err))
	}
	return o
}

func findService(dev *bluetooth.Device, id bluetooth.UUID) (*bluetooth.DeviceService, error) {
	svcs, err := dev.DiscoverServices(nil)
	if err != nil {
		return nil, fmt.Errorf("could not discover services: %w", err)
	}
	for i := range svcs {
		if svcs[i].UUID() == uuidDeviceService {
			return &svcs[i], nil
		}
	}

	return nil, errNoSvc
}

func (dev *Device) devCharByUUID(id bluetooth.UUID) (*bluetooth.DeviceCharacteristic, error) {
	if dev.svc == nil {
		return nil, errNoSvc
	}

	chars, err := dev.svc.DiscoverCharacteristics([]bluetooth.UUID{id})
	if err != nil {
		return nil, fmt.Errorf("could not get characteristic %q: %w", uuidCommonReadSWRevision, err)
	}

	for i := range chars {
		char := chars[i]
		if char.UUID() == id {
			return &char, nil
		}
	}

	return nil, fmt.Errorf("could not get characteristic for %q", id)
}
