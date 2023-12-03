package elastic_test

import (
	"bytes"
	"io"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "ztbus/elastic"
)

var _ = Describe("Bulki", func() {
	var (
		chk int
		bi  *Bulki
	)

	Describe("creating a new Bulki iterator", func() {
		JustBeforeEach(func() {
			bi = NewBulki(chk, nil)
		})

		When("all is well", func() {
			BeforeEach(func() {
				chk = 2
			})

			It("does not set error", func() {
				Expect(bi.Err()).ToNot(HaveOccurred())
			})
		})

		When("chunk size is invalid", func() {
			BeforeEach(func() {
				chk = 0
			})

			It("sets error and does not iterate", func() {
				Expect(bi.Err()).To(HaveOccurred())
				Expect(bi.Next()).To(BeFalse())
			})
		})
	})

	Describe("getting a chunk from Bulki", func() {
		var (
			val io.Reader
		)

		JustBeforeEach(func() {
			next := bi.Next()
			Expect(next).To(BeTrue())
			val = bi.Value()
		})

		When("all is well", func() {
			BeforeEach(func() {
				bi = NewBulki(2, bytes.NewBufferString(data))
			})

			It("produces the first chunk", func() {
				Expect(bi.Err()).ToNot(HaveOccurred())

				buf := val.(*bytes.Buffer)
				Expect(buf.String()).To(Equal("{\"index\":{}}\n{\"thing\":\"one\"}\n{\"index\":{}}\n{\"thing\":\"two\"}\n"))

				Expect(bi.Count()).To(Equal(2))
				Expect(bi.Skipped()).To(Equal([][]byte{[]byte("not the json")}))
			})
		})
	})

	Describe("scanning thru Bulki's input", func() {
		var (
			val io.Reader
			out []string
		)

		JustBeforeEach(func() {
			for bi.Next() {
				val = bi.Value()

				buf := val.(*bytes.Buffer)
				out = append(out, buf.String())
			}
		})

		When("all is well", func() {
			BeforeEach(func() {
				bi = NewBulki(2, bytes.NewBufferString(data))
			})

			It("produces the interlaced chunks", func() {
				Expect(bi.Err()).ToNot(HaveOccurred())

				Expect(out).To(HaveLen(2))
				Expect(out[0]).To(Equal("{\"index\":{}}\n{\"thing\":\"one\"}\n{\"index\":{}}\n{\"thing\":\"two\"}\n"))
				Expect(out[1]).To(Equal("{\"index\":{}}\n{\"thing\":\"three\"}\n"))

				Expect(bi.Count()).To(Equal(3))
				Expect(bi.Skipped()).To(Equal([][]byte{[]byte("not the json"), []byte("not the json too")}))
			})
		})
	})

	// Todo: think about triggering error and edge cases

})

var data = `{"thing":"one"}
not the json
{"thing":"two"}
not the json too
{"thing":"three"}`
