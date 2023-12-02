package elastic_test

import (
	"bytes"
	"context"
	"fmt"
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

	Describe("working with ES", func() {
		BeforeEach(func() {
			es, err = cfg.New(client, fs)
			Expect(err).ToNot(HaveOccurred())
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
					client.SendObjectFunc = func(ctx context.Context, method string, path string, snd any, rcv any) error {
						result := rcv.(*DocResult)
						result.Result = "created"
						return nil
					}
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

			When("ES reports non-create", func() {
				BeforeEach(func() {
					client.SendObjectFunc = func(ctx context.Context, method string, path string, snd any, rcv any) error {
						result := rcv.(*DocResult)
						result.Result = "bargle"
						return nil
					}
				})

				It("returns an error", func() {
					Expect(err).To(HaveOccurred())
					Expect(client.SendObjectCalls()).To(HaveLen(1))
				})
			})

			When("client returns error", func() {
				BeforeEach(func() {
					client.SendObjectFunc = func(ctx context.Context, method string, path string, snd any, rcv any) error {
						return fmt.Errorf("oops")
					}
				})

				It("returns an error", func() {
					Expect(err).To(HaveOccurred())
					Expect(client.SendObjectCalls()).To(HaveLen(1))
				})
			})
		})

		Describe("posting bulk data", func() {
			var (
				data *bytes.Buffer
			)

			BeforeEach(func() {
				data = bytes.NewBufferString(`{"ima": "pc"}`)
			})

			JustBeforeEach(func() {
				err = es.PostBulk(ctx, data)
			})

			When("all is well", func() {
				BeforeEach(func() {
					client.SendJsonFunc = func(ctx context.Context, method string, path string, body io.Reader) ([]byte, error) {
						return []byte(`{"errors":false}`), nil
					}
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

			When("es reports errors", func() {
				BeforeEach(func() {
					client.SendJsonFunc = func(ctx context.Context, method string, path string, body io.Reader) ([]byte, error) {
						return []byte(`{"errors":true}`), nil
					}
				})

				It("returns an error", func() {
					Expect(err).To(HaveOccurred())
					Expect(client.SendJsonCalls()).To(HaveLen(1))
				})
			})

			When("client return error", func() {
				BeforeEach(func() {
					client.SendJsonFunc = func(ctx context.Context, method string, path string, body io.Reader) ([]byte, error) {
						return []byte(`{"errors":false}`), fmt.Errorf("oops")
					}
				})

				It("returns an error", func() {
					Expect(err).To(HaveOccurred())
					Expect(client.SendJsonCalls()).To(HaveLen(1))
				})
			})
		})

		Describe("searching", func() {
			var (
				query    []byte
				response []byte
			)

			JustBeforeEach(func() {
				response, err = es.Search(ctx, query)
			})

			When("all is well", func() {
				BeforeEach(func() {
					client.SendJsonFunc = func(ctx context.Context, method string, path string, body io.Reader) ([]byte, error) {
						return []byte(`{"number":42}`), nil
					}
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
})
