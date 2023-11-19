package template_test

import (
	"io/fs"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "ztbus/template"
)

func TestTemplate(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Template Suite")
}

var _ = Describe("Template", func() {
	var (
		tmpl *Template
		fs   fs.FS
		err  error
	)

	BeforeEach(func() {
		tmpl = &Template{
			Suffix: "yaml",
		}
		fs = os.DirFS("../test/templates")
	})

	Describe("loading templates from fs", func() {
		JustBeforeEach(func() {
			err = tmpl.Load(fs)
		})

		When("all is well", func() {
			It("loads the expected number of templates", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(tmpl.Tmpl.Templates()).To(HaveLen(1))
			})
		})
	})

	Describe("rendering a template", func() {
		var (
			name string
			data map[string]string
		)

		BeforeEach(func() {
			err = tmpl.Load(fs)
			Expect(err).ToNot(HaveOccurred())

			name = "avgspeed"
			data = map[string]string{
				"interval": "5m",
				"bgn":      "2022-09-21T08:00:00Z",
				"end":      "2022-09-21T16:59:59.999Z",
			}
		})

		Describe("to string", func() {
			var (
				out string
			)

			JustBeforeEach(func() {
				out, err = tmpl.RenderString(name, data)
			})

			When("all is well", func() {
				It("contains the expected substring", func() {
					Expect(err).ToNot(HaveOccurred())
					Expect(out).To(ContainSubstring(`    gte: "2022-09-21T08:00:00Z"`))
				})
			})
		})

		Describe("from yaml to json", func() {
			var (
				out []byte
			)

			JustBeforeEach(func() {
				out, err = tmpl.RenderJson(name, data)
			})

			When("all is well", func() {
				It("contains the expected substring", func() {
					Expect(err).ToNot(HaveOccurred())
					Expect(out).To(ContainSubstring(`"filter":[{"range":{"ts":{"gte":"2022-09-21T08:00:00Z"`))
				})
			})
		})

	})

})
