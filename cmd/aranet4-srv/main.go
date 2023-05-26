// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main // import "sbinet.org/x/aranet4/cmd/aranet4-srv"

import (
	"flag"
	"log"
	"net/http"

	"sbinet.org/x/aranet4"
)

func main() {
	log.SetPrefix("aranet4: ")
	log.SetFlags(0)

	var (
		addr = flag.String("addr", ":8080", "[host]:addr to serve")
		db   = flag.String("db", "data.db", "path to DB file")
	)

	flag.Parse()

	xmain(*addr, *db)
}

func xmain(addr, db string) {
	srv, err := aranet4.NewServer("/", db)
	if err != nil {
		log.Panicf("could not create aranet4 server: %+v", err)
	}
	defer srv.Close()

	log.Printf("serving %q...", addr)
	err = http.ListenAndServe(addr, srv)
	if err != nil {
		log.Panicf("could not serve %q: %+v", addr, err)
	}
}
