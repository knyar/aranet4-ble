// Copyright ©2023 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package aranet4

import (
	"github.com/muka/go-bluetooth/bluez/profile/gatt"
)

func (dev *Device) devCharByUUID(id string) (*gatt.GattCharacteristic1, error) {
	return dev.dev.GetCharByUUID(id)
}
