package elastic_test

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "ztbus/elastic"
	"ztbus/elastic/mock"
)

func TestElastic(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Elastic Suite")
}

// Todo: unit err path

var _ = Describe("Elastic", func() {
	var (
		cfg    *Config
		client *mock.ClientMock
		fs     fs.FS
		es     *Elastic
		err    error

		ctx context.Context
	)

	BeforeEach(func() {
		cfg = &Config{
			Idx: "test-index",
		}
		client = &mock.ClientMock{}
		fs = os.DirFS("../test/templates")

		ctx = context.Background()
	})

	Describe("creating a new Elastic client", func() {
		JustBeforeEach(func() {
			es, err = cfg.New(client, fs)
		})

		When("all is well", func() {
			It("does not error and populates", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(es.Idx).To(Equal("test-index"))
				Expect(es.Client).To(Equal(client))
			})
		})
	})

	Describe("inserting a document object", func() {
		var (
			obj map[string]string
		)

		JustBeforeEach(func() {
			err = es.Insert(ctx, obj)
		})

		When("all is well", func() {
			BeforeEach(func() {
				client = &mock.ClientMock{
					SendObjectFunc: func(ctx context.Context, method string, path string, snd any, rcv any) error {
						result := rcv.(*DocResult)
						result.Result = "created"
						return nil
					},
				}
				es, err = cfg.New(client, fs)
				Expect(err).ToNot(HaveOccurred())

				obj = map[string]string{"ima": "pc"}
			})

			It("does not error and calls SendObject properly", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(client.SendObjectCalls()).To(HaveLen(1))

				call := client.SendObjectCalls()[0]
				Expect(call.Method).To(Equal("POST"))
				Expect(call.Path).To(Equal("/test-index/_doc"))
				Expect(call.Snd).To(Equal(obj))
			})
		})
	})

	Describe("posting bulk data", func() {
		var (
			data *bytes.Buffer
		)

		JustBeforeEach(func() {
			err = es.PostBulk(ctx, data)
		})

		When("all is well", func() {
			BeforeEach(func() {
				client = &mock.ClientMock{
					SendJsonFunc: func(ctx context.Context, method string, path string, body io.Reader) ([]byte, error) {
						return []byte(`{"errors":false}`), nil
					},
				}
				es, err = cfg.New(client, fs)
				Expect(err).ToNot(HaveOccurred())

				data = bytes.NewBufferString(`{"ima": "pc"}`)
			})

			It("does not error and calls SendJson properly", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(client.SendJsonCalls()).To(HaveLen(1))

				call := client.SendJsonCalls()[0]
				Expect(call.Method).To(Equal("POST"))
				Expect(call.Path).To(Equal("/test-index/_bulk"))
				Expect(call.Body.(*bytes.Buffer).String()).To(Equal("{\"ima\": \"pc\"}"))
			})
		})
	})

	Describe("searching ES", func() {
		var (
			query    []byte
			response []byte
		)

		JustBeforeEach(func() {
			response, err = es.Search(ctx, query)
		})

		When("all is well", func() {
			BeforeEach(func() {
				client = &mock.ClientMock{
					SendJsonFunc: func(ctx context.Context, method string, path string, body io.Reader) ([]byte, error) {
						return []byte(`{"number":42}`), nil
					},
				}
				es, err = cfg.New(client, fs)
				Expect(err).ToNot(HaveOccurred())

				query = []byte(`{"ima": "pc"}`)
			})

			It("does not error and calls SendJson properly", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(client.SendJsonCalls()).To(HaveLen(1))

				call := client.SendJsonCalls()[0]
				Expect(call.Method).To(Equal("GET"))
				Expect(call.Path).To(Equal("/test-index/_search"))
				Expect(call.Body.(*bytes.Buffer).String()).To(Equal("{\"ima\": \"pc\"}"))

				Expect(response).To(Equal([]byte(`{"number":42}`)))
			})
		})
	})

})
