package elastic

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

var (
	action  = []byte(`{"index":{}}`)
	newline = []byte("\n")
	fixed   = 2*len(newline) + len(action)
)

// Bulki iterates over a reader, adding ES bulk action lines before each valid line of json.
type Bulki struct {
	scn *bufio.Scanner
	cnt int
	chk int
	buf *bytes.Buffer
	skp [][]byte
	err error
}

// NewBulki creates a new Bulki.
func NewBulki(chunkSize int, rdr io.Reader) *Bulki {

	var err error
	if chunkSize < 1 {
		err = errors.Errorf("chunk size must be positive, got: %d", chunkSize)
	}

	return &Bulki{
		scn: bufio.NewScanner(rdr),
		chk: chunkSize,
		buf: &bytes.Buffer{},
		err: err,
	}
}

// Next reads the next chunk and adds action lines.
func (blk *Bulki) Next() bool {

	// reset output buffer and scan reader

	blk.buf.Reset()
	for blk.scn.Scan() {

		// skip non-json

		data := blk.scn.Bytes()
		if !json.Valid(data) {
			blk.skp = append(blk.skp, data)
			continue
		}

		// buffer action-source and check for chunk boundary

		blk.interlace(data)

		blk.cnt++
		if blk.cnt%blk.chk == 0 {
			break
		}
	}

	// decide if there's more value, har

	err := blk.scn.Err()
	if err != nil {
		blk.err = errors.Wrapf(err, "failed to scan reader")
		return false
	}
	if blk.buf.Len() == 0 {
		return false
	}
	return true
}

// Value returns the output buffer for read.
func (blk *Bulki) Value() io.Reader {
	return blk.buf
}

// Err returns any error encountered.
func (blk *Bulki) Err() error {
	return blk.err
}

// Count reports number of json lines handled.
func (blk *Bulki) Count() int {
	return blk.cnt
}

// Skipped reports any lines that have been skipped.
func (blk *Bulki) Skipped() [][]byte {
	return blk.skp
}

// unexported

func (blk *Bulki) interlace(data []byte) {

	blk.buf.Grow(len(data) + fixed)
	blk.buf.Write(action)
	blk.buf.Write(newline)
	blk.buf.Write(data)
	blk.buf.Write(newline)
}
