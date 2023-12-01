package elastic_test

import (
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
		client = &mock.ClientMock{
			SendJsonFunc: func(ctx context.Context, method string, path string, body io.Reader) ([]byte, error) {
				panic("mock out the SendJson method")
			},
			SendObjectFunc: func(ctx context.Context, method string, path string, snd any, rcv any) error {
				//panic("mock out the SendObject method")
				fmt.Printf(">>> %s\n", method)
				fmt.Printf(">>> %#v\n", rcv)
				result := rcv.(*DocResult)
				result.Result = "created"

				return nil
			},
		}
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

	Describe("creating a new Elastic client", func() {
		var (
			data []byte
		)

		JustBeforeEach(func() {
			err = es.Insert(ctx, data)
		})

		When("all is well", func() {
			BeforeEach(func() {
				es, err = cfg.New(client, fs)
				Expect(err).ToNot(HaveOccurred())

				data = []byte(`{"ima":"pc"}`) // Todo: move up
			})

			FIt("does not error and ...", func() {
				Expect(err).ToNot(HaveOccurred())
				//Expect(es.Idx).To(Equal("test-index"))
			})
		})
	})

})

// ClientMock is a mock implementation of elastic.Client.
//
//	func TestSomethingThatUsesClient(t *testing.T) {
//
//		// make and configure a mocked elastic.Client
//		mockedClient := &ClientMock{
//			SendJsonFunc: func(ctx context.Context, method string, path string, body io.Reader) ([]byte, error) {
//				panic("mock out the SendJson method")
//			},
//			SendObjectFunc: func(ctx context.Context, method string, path string, snd any, rcv any) error {
//				panic("mock out the SendObject method")
//			},
//		}
//
//		// use mockedClient in code that requires elastic.Client
//		// and then make assertions.
//
//	}
