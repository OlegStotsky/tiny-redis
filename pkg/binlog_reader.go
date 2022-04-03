package pkg

import (
	"bufio"
	"encoding/binary"
	"io"
)

type binlogReader struct {
	r   *bufio.Reader
	b32 [4]byte
	b64 [8]byte
}

func newBinlogReader(r *bufio.Reader) *binlogReader {
	return &binlogReader{r: r}
}

func (c *binlogReader) ReadUInt32() (uint32, error) {
	_, err := io.ReadFull(c.r, c.b32[:])
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(c.b32[:]), nil
}

func (c *binlogReader) ReadUInt64() (uint64, error) {
	_, err := io.ReadFull(c.r, c.b64[:])
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(c.b64[:]), nil
}

func (c *binlogReader) ReadFull(b []byte) error {
	_, err := io.ReadFull(c.r, b)
	if err != nil {
		return err
	}

	return nil
}
