package ztbus_test

import (
	"testing"
	"time"
	"ztbus"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "ztbus"
)

func TestZtBus(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ZtBus Suite")
}

var _ = Describe("ZtBus", func() {
	var (
		filename string
		ztb      *ZtBusCols
		err      error
	)
	BeforeEach(func() {
		filename = "test/data/B183_2019-06-24_03-16-13_2019-06-24_18-54-06.csv"
	})

	Describe("loading data from csv", func() {
		JustBeforeEach(func() {
			ztb, err = New(filename)
		})

		When("all is well", func() {
			It("has the expected length", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(ztb.BusId).To(Equal("B183"))
				Expect(ztb.Ts).To(HaveLen(56274))

				then, _ := time.Parse(time.RFC3339, "2019-06-24T03:16:13Z")
				Expect(ztb.Row(0)).To(Equal(&ztbus.ZtBus{
					BusId:          "B183",
					Ts:             then,
					Power:          0,
					Altitude:       0.8269722,
					RouteName:      "-",
					PassengerCount: 0,
					VehicleSpeed:   0,
					TractionForce:  0,
				}))
			})
		})
	})
})
