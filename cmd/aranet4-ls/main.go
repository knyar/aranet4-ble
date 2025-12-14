// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/knyar/aranet4-ble"
	"github.com/rigado/ble"
	"github.com/rigado/ble/linux"
)

func main() {
	log.SetPrefix("aranet4: ")
	log.SetFlags(0)

	var (
		hciSkt  = flag.Int("device", -1, "bluetooth device hci index")
		addr    = flag.String("addr", "F5:6C:BE:D5:61:47", "MAC address of Aranet4")
		verbose = flag.Bool("v", false, "enable verbose mode")
	)

	flag.Parse()

	d, err := linux.NewDevice(
		ble.OptTransportHCISocket(*hciSkt),
		ble.OptDialerTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("can't create new device: %v", err)
	}
	ble.SetDefaultDevice(d)

	dev, err := aranet4.New(context.Background(), *addr)
	if err != nil {
		log.Fatalf("could not create aranet4 client: %+v", err)
	}
	defer dev.Close()

	if *verbose {
		log.Printf("name: %q", dev.Name())

		vers, err := dev.Version()
		if err != nil {
			log.Fatalf("could not get device version: %+v", err)
		}
		log.Printf("vers: %q", vers)
	}

	data, err := dev.Read()
	if err != nil {
		log.Fatalf("could not run client: %+v", err)
	}
	fmt.Printf("%v", data)

	err = dev.Close()
	if err != nil {
		log.Fatalf("could not close client: %+v", err)
	}
}
