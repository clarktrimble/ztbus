// Package ztbus defines ZTBus related structs and handles the thankless
// job of taking apart its csv files.
package ztbus

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// AvgSpeed represents a bus's speed at a particular time.
type AvgSpeed struct {
	Ts           time.Time `json:"ts"`
	BusId        string    `json:"bus_id"`
	VehicleSpeed float64   `json:"vehicle_speed"`
}

// AvgSpeeds is a multiplicity of them.
type AvgSpeeds []AvgSpeed

// String returns a tsv dump.
func (avgs AvgSpeeds) String() string {

	bldr := &strings.Builder{}
	for _, avg := range avgs {
		fmt.Fprintf(bldr, "%s\t%s\t%f\n", avg.Ts.Format(time.RFC3339), avg.BusId, avg.VehicleSpeed)
	}

	return bldr.String()
}

// ZtBus represents a row from as taken from csv file.
type ZtBus struct {
	BusId          string    `json:"bus_id"`
	Ts             time.Time `json:"ts"`
	Power          float64   `json:"power"`
	Altitude       float64   `json:"altitude"`
	RouteName      string    `json:"route_name"`
	PassengerCount float64   `json:"passenger_count"`
	VehicleSpeed   float64   `json:"vehicle_speed"`
	TractionForce  float64   `json:"traction_force"`
}

// String returns json.
func (ztb *ZtBus) String() string {

	out, err := json.Marshal(ztb)
	if err != nil {
		return "failed to marshal ztb"
	}

	return string(out)
}

// ZtBusCols is a column-oriented view of ZTBus data.
type ZtBusCols struct {
	Len            int
	ColIdx         map[string]int
	BusId          string
	Ts             []time.Time
	Power          []float64
	Altitude       []float64
	RouteName      []string
	PassengerCount []float64
	VehicleSpeed   []float64
	TractionForce  []float64
}

// Row returns a ZtBus obj for a given row.
func (ztb *ZtBusCols) Row(i int) *ZtBus {

	// Todo: check len

	return &ZtBus{
		BusId:          ztb.BusId,
		Ts:             ztb.Ts[i],
		Power:          ztb.Power[i],
		Altitude:       ztb.Altitude[i],
		RouteName:      ztb.RouteName[i],
		PassengerCount: ztb.PassengerCount[i],
		VehicleSpeed:   ztb.VehicleSpeed[i],
		TractionForce:  ztb.TractionForce[i],
	}
}

// String returns json.
func (ztb *ZtBusCols) String() string {

	out, err := json.MarshalIndent(ztb, "", "  ")
	if err != nil {
		return "somehow failed to marshal ztb"
	}

	return string(out)
}

// New creates ZtBusCols given a csv filename.
func New(fn string) (ztb *ZtBusCols, err error) {

	// where fn is expected to look like "data/B183_2019-06-24_03-16-13_2019-06-24_18-54-06.csv"
	ztb = &ZtBusCols{
		BusId:  strings.Split(filepath.Base(fn), "_")[0],
		ColIdx: map[string]int{},
	}

	file, err := os.Open(fn)
	if err != nil {
		err = errors.Wrapf(err, "failed to open: %s", fn)
		return
	}

	rdr := csv.NewReader(file)
	rdr.ReuseRecord = true

	// get header and populate ColIdx

	record, err := rdr.Read()
	if err != nil {
		err = errors.Wrapf(err, "failed to read: %s", fn)
		return
	}

	for i, field := range record {
		ztb.ColIdx[field] = i
	}

	// append the rest

	for {
		record, err = rdr.Read()
		if err == io.EOF {
			err = nil
			return
		}
		if err != nil {
			return
		}

		err = ztb.appendRecord(record)
		if err != nil {
			return
		}
	}
}

// unexported

func (ztb *ZtBusCols) appendRecord(record []string) (err error) {

	ts, err := time.Parse(time.RFC3339, record[ztb.ColIdx["time_iso"]])
	if err != nil {
		err = errors.Wrapf(err, "failed to parse ts")
		return
	}
	ztb.Ts = append(ztb.Ts, ts)

	power, err := ztb.parseFloat("electric_powerDemand", record)
	if err != nil {
		return
	}
	ztb.Power = append(ztb.Power, power)

	altitude, err := ztb.parseFloat("gnss_latitude", record)
	if err != nil {
		return
	}
	ztb.Altitude = append(ztb.Altitude, altitude)

	ztb.RouteName = append(ztb.RouteName, record[ztb.ColIdx["itcs_busRoute"]])

	passengers, err := ztb.parseFloat("itcs_numberOfPassengers", record)
	if err != nil {
		return
	}
	ztb.PassengerCount = append(ztb.PassengerCount, passengers)

	speed, err := ztb.parseFloat("odometry_vehicleSpeed", record)
	if err != nil {
		return
	}
	ztb.VehicleSpeed = append(ztb.VehicleSpeed, speed)

	force, err := ztb.parseFloat("traction_tractionForce", record)
	if err != nil {
		return
	}
	ztb.TractionForce = append(ztb.TractionForce, force)

	ztb.Len++
	return
}

func (ztb *ZtBusCols) parseFloat(field string, record []string) (val float64, err error) {

	val, err = strconv.ParseFloat(record[ztb.ColIdx[field]], 64)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse %s", field)
		return
	}

	if val == -0 {
		val = 0
	}
	if math.IsNaN(val) {
		val = 0
		// Todo: maybe return a nan bool??
	}

	return
}

/*
time_iso
time_unix
electric_powerDemand
gnss_altitude
gnss_course
gnss_latitude
gnss_longitude
itcs_busRoute
itcs_numberOfPassengers
itcs_stopName
odometry_articulationAngle
odometry_steeringAngle
odometry_vehicleSpeed
odometry_wheelSpeed_fl
odometry_wheelSpeed_fr
odometry_wheelSpeed_ml
odometry_wheelSpeed_mr
odometry_wheelSpeed_rl
odometry_wheelSpeed_rr
status_doorIsOpen
status_gridIsAvailable
status_haltBrakeIsActive
status_parkBrakeIsActive
temperature_ambient
traction_brakePressure
traction_tractionForce
*/
