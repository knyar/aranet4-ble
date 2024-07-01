// Copyright ©2022 The aranet4 Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package arasrv // import "sbinet.org/x/aranet4/arasrv"

import (
	"bytes"
	"fmt"
	"image/color"
	"math"

	"git.sr.ht/~sbinet/epok"
	"go-hep.org/x/hep/hplot"
	"golang.org/x/sync/errgroup"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
	"gonum.org/v1/plot/vg/vgimg"
	"sbinet.org/x/aranet4"
)

// Message holds informations about a device.
type Message struct {
	Root     string   `json:"root"`
	Devices  []string `json:"devices"`
	DeviceID string   `json:"device_id"`
	Status   string   `json:"status"`
	Refresh  int      `json:"refresh"`
	From     string   `json:"from"`
	To       string   `json:"to"`
	Plots    struct {
		H   string `json:"h"`
		P   string `json:"p"`
		T   string `json:"t"`
		CO2 string `json:"co2"`
	} `json:"plots"`
}

var (
	tcnv epok.UTCUnixTimeConverter
)

func (srv *manager) plot(data []aranet4.Data) error {
	var err error

	xs := make([]float64, 0, len(data))
	for _, v := range data {
		xs = append(xs, tcnv.FromTime(v.Time))
	}

	var grp errgroup.Group
	grp.Go(func() error {
		err := srv.plotCO2(xs, data)
		if err != nil {
			return fmt.Errorf("could not create CO2 plot: %w", err)
		}
		return nil
	})

	grp.Go(func() error {
		err := srv.plotT(xs, data)
		if err != nil {
			return fmt.Errorf("could not create T plot: %w", err)
		}
		return nil
	})

	grp.Go(func() error {
		err := srv.plotH(xs, data)
		if err != nil {
			return fmt.Errorf("could not create H plot: %w", err)
		}
		return nil
	})

	grp.Go(func() error {
		err := srv.plotP(xs, data)
		if err != nil {
			return fmt.Errorf("could not create P plot: %w", err)
		}
		return nil
	})

	err = grp.Wait()
	if err != nil {
		return fmt.Errorf("could not create plots: %w", err)
	}

	return nil
}

func (srv *manager) plotCO2(xs []float64, data []aranet4.Data) error {
	var (
		ys = make([]float64, 0, len(data))
	)

	for _, data := range data {
		ys = append(ys, float64(data.CO2))
	}

	c := color.NRGBA{B: 255, A: 255}
	return srv.genPlot(&srv.plots.CO2, xs, ys, "CO2 [ppm]", c)
}

func (srv *manager) plotT(xs []float64, data []aranet4.Data) error {
	var (
		ys = make([]float64, 0, len(data))
	)

	for _, data := range data {
		ys = append(ys, float64(data.T))
	}

	c := color.NRGBA{R: 255, A: 255}
	return srv.genPlot(&srv.plots.T, xs, ys, "T [°C]", c)
}

func (srv *manager) plotH(xs []float64, data []aranet4.Data) error {
	var (
		ys = make([]float64, 0, len(data))
	)

	for _, data := range data {
		ys = append(ys, float64(data.H))
	}

	c := color.NRGBA{G: 255, A: 255}
	return srv.genPlot(&srv.plots.H, xs, ys, "Humidity [%]", c)
}

func (srv *manager) plotP(xs []float64, data []aranet4.Data) error {
	var (
		ys = make([]float64, 0, len(data))
	)

	for _, data := range data {
		ys = append(ys, float64(data.P))
	}

	c := color.NRGBA{B: 255, G: 255, A: 255}
	return srv.genPlot(&srv.plots.P, xs, ys, "Atmospheric Pressure [hPa]", c)
}

func (srv *manager) genPlot(buf *bytes.Buffer, xs, ys []float64, label string, c color.NRGBA) error {

	buf.Reset()

	plt := hplot.New()
	plt.Title.Text = "Device: " + srv.id
	plt.Y.Label.Text = label
	plt.X.Tick.Marker = epok.Ticks{
		Converter: tcnv,
		Format:    "2006-01-02\n15:04",
	}

	sca, err := hplot.NewScatter(hplot.ZipXY(xs, ys))
	if err != nil {
		return fmt.Errorf("could not create CO2 scatter plot: %w", err)
	}

	c1 := c
	c2 := c
	c2.A = 38

	sca.GlyphStyle.Color = c1
	sca.GlyphStyle.Radius = 2
	sca.GlyphStyle.Shape = draw.CircleGlyph{}

	lin, err := hplot.NewLine(hplot.ZipXY(xs, ys))
	if err != nil {
		return fmt.Errorf("could not create CO2 line plot: %w", err)
	}
	lin.LineStyle.Color = c1
	lin.FillColor = c2

	plt.Add(hplot.NewGrid(), lin, sca)

	const size = 20 * vg.Centimeter
	cnv := vgimg.PngCanvas{
		Canvas: vgimg.New(vg.Length(math.Phi)*size, size),
	}
	plt.Draw(draw.New(cnv))
	_, err = cnv.WriteTo(buf)
	if err != nil {
		return fmt.Errorf("could not create CO2 plot: %w", err)
	}

	return nil
}

const page = `
<html>
	<head>
		<title>Aranet4 monitoring</title>
		<meta http-equiv="refresh" content="{{.Refresh}}">
	</head>

	<body>
{{- if .Devices}}
		<h2>Devices</h2>
		<ul>
{{- with $ctx := .}}
{{- range .Devices}}
			<li><a href="{{$ctx.Root}}?device_id={{.}}&from={{$ctx.From}}&to={{$ctx.To}}">{{.}}</a></li>
{{- end}}
{{- end}}
		</ul>
{{- end}}
		<pre>
Device:      {{.DeviceID}}
{{.Status}}
		</pre>
		<!-- CO2 -->
		<hr>
        <div class="row align-items-center justify-content-center">
		  <img src="{{.Root}}plot-co2"/>
        </div>

		<!-- Temperature -->
		<hr>
        <div class="row align-items-center justify-content-center">
		  <img src="{{.Root}}plot-t"/>
        </div>
		
		<!-- Humidity -->
		<hr>
        <div class="row align-items-center justify-content-center">
		  <img src="{{.Root}}plot-h"/>
        </div>

		<!-- Pressure -->
		<hr>
        <div class="row align-items-center justify-content-center">
		  <img src="{{.Root}}plot-p"/>
        </div>
	</body>
</html>
`
