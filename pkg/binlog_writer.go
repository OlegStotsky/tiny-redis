package pkg

import (
	"bufio"
	"encoding/binary"
)

type binlogWriter struct {
	w   *bufio.Writer
	b32 [4]byte
}

func newBinaryWriter(w *bufio.Writer) *binlogWriter {
	return &binlogWriter{w: w}
}

func (c *binlogWriter) writeUInt32(x uint32) error {
	binary.BigEndian.PutUint32(c.b32[:], x)
	_, err := c.w.Write(c.b32[:])
	return err
}

func (c *binlogWriter) writeBytes(b []byte) error {
	_, err := c.w.Write(b)
	return err
}

func (c *binlogWriter) flush() error {
	return c.w.Flush()
}
