// Copyright ©2023 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4

import (
	"fmt"

	"github.com/rigado/ble"
)

func (dev *Device) devCharByUUID(id string) (*ble.Characteristic, error) {
	uuid, err := ble.Parse(id)
	if err != nil {
		return nil, err
	}
	char := dev.profile.FindCharacteristic(&ble.Characteristic{UUID: uuid})
	if char == nil {
		return nil, fmt.Errorf("characteristic %q not found", id)
	}
	return char, nil
}
