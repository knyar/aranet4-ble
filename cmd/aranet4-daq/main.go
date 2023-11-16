// Copyright ©2023 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command aranet4-daq retrieves data from an Aranet4 device and
// uploads it to an HTTP server.
package main // import "sbinet.org/x/aranet4/cmd/aranet4-daq"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
	"sbinet.org/x/aranet4"
)

func main() {
	log.SetPrefix("aranet4-daq: ")
	log.SetFlags(0)

	var (
		ep    = flag.String("endpoint", "", "endpoint where to POST data")
		devID = flag.String("device", "F5:6C:BE:D5:61:47", "MAC address of Aranet4")
	)

	flag.Parse()

	if *ep == "" {
		flag.Usage()
		log.Fatalf("missing endpoint")
	}

	err := xmain(*ep, *devID)
	if err != nil {
		log.Fatal(err)
	}
}

func xmain(endpoint, devID string) error {
	n := 360 // ~1 hour
retry:
	srv, err := run(10*time.Second, func() (*server, error) {
		return newServer(endpoint, devID)
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && n > 0 {
			n--
			log.Printf("could not create DAQ server: %+v", err)
			time.Sleep(10 * time.Second)
			goto retry
		}
		return fmt.Errorf("could not create DAQ server: %w", err)
	}

	return srv.run()
}

type server struct {
	ep string
	id string

	freq time.Duration
	http *http.Client
}

func newServer(ep, id string) (*server, error) {
	log.Printf("creating initial aranet4 device...")
	dev, err := run(5*time.Second, func() (*aranet4.Device, error) {
		return aranet4.New(context.Background(), id)
	})
	if err != nil {
		return nil, fmt.Errorf("could not create aranet4 device: %w", err)
	}
	defer dev.Close()
	log.Printf("creating initial aranet4 device... [done]")

	log.Printf("retrieving aranet4 device refresh interval...")
	freq, err := run(5*time.Second, dev.Interval)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve data refresh interval: %w", err)
	}
	log.Printf("retrieving aranet4 device refresh interval... [done] (freq=%v)", freq)

	srv := &server{
		ep:   ep,
		id:   id,
		freq: freq,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	return srv, nil
}

func (srv *server) run() error {
	log.Printf("retrieving historical data...")
	vs, err := srv.readn()
	if err != nil {
		return fmt.Errorf("could not retrieve historical data: %w", err)
	}
	log.Printf("retrieving historical data... [done]")

	err = srv.upload(vs...)
	if err != nil {
		return fmt.Errorf("could not upload historical data: %w", err)
	}

	tck := time.NewTicker(srv.freq)
	defer tck.Stop()

	for range tck.C {
		v, err := srv.read()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				log.Printf("could not retrieve data: %+v", err)
				continue
			}
			return fmt.Errorf("could not retrieve data: %w", err)
		}

		err = srv.upload(v)
		if err != nil {
			return fmt.Errorf("could not upload data: %w", err)
		}
	}

	return nil
}

func (srv *server) readn() ([]aranet4.Data, error) {
	return run(10*time.Second, func() ([]aranet4.Data, error) {
		log.Printf("connecting to aranet4 device...")
		dev, err := aranet4.New(context.Background(), srv.id)
		if err != nil {
			return nil, fmt.Errorf("could not create aranet4 device: %w", err)
		}
		defer dev.Close()
		log.Printf("connecting to aranet4 device... [done]")

		return dev.ReadAll()
	})
}

func (srv *server) read() (aranet4.Data, error) {
	return run(5*time.Second, func() (aranet4.Data, error) {
		dev, err := aranet4.New(context.Background(), srv.id)
		if err != nil {
			return aranet4.Data{}, fmt.Errorf("could not create aranet4 device: %w", err)
		}
		defer dev.Close()

		return dev.Read()
	})
}

func (srv *server) upload(vs ...aranet4.Data) error {
	if len(vs) == 0 {
		return nil
	}
	log.Printf("uploading %d data points...", len(vs))

	data := struct {
		DevID string         `json:"device_id"`
		Data  []aranet4.Data `json:"data"`
	}{
		DevID: srv.id,
		Data:  vs,
	}

	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(data)
	if err != nil {
		return fmt.Errorf("could not encode data to JSON: %w", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.ep, buf)
	if err != nil {
		return fmt.Errorf("could not create HTTP request to %q: %w", srv.ep, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := srv.http.Do(req)
	if err != nil {
		return fmt.Errorf("could not POST request to %q: %w", srv.ep, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("could not upload JSON payload: %s (%d)", resp.Status, resp.StatusCode)
	}

	log.Printf("uploading %d data points... [done]", len(vs))
	return nil
}

func run[T any](timeout time.Duration, f func() (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var (
		v      T
		grp, _ = errgroup.WithContext(ctx)
	)

	grp.Go(func() error {
		var err error
		v, err = f()
		return err
	})

	ch := make(chan error, 1)
	go func() {
		ch <- grp.Wait()
	}()

	select {
	case err := <-ch:
		return v, err
	case <-ctx.Done():
		return v, ctx.Err()
	}
}
