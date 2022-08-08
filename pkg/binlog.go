package pkg

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"
)

const (
	fsyncTime = 1 * time.Second
)

type binlogHandler = func(magic uint32, rdr *binlogReader) error

type Binlog struct {
	f         *os.File
	binWriter *binlogWriter
	handler   binlogHandler
	logger    *zap.SugaredLogger
}

func NewBinlog(filePath string, handler binlogHandler, logger *zap.SugaredLogger) (*Binlog, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}
	bufWriter := bufio.NewWriter(file)
	binWriter := newBinaryWriter(bufWriter)

	return &Binlog{
		binWriter: binWriter,
		f:         file,
		handler:   handler,
		logger:    logger,
	}, nil
}

func (c *Binlog) Open() error {
	c.logger.Debugf("opening binlog...")

	rdr := bufio.NewReader(c.f)
	binReader := newBinlogReader(rdr)

	go c.fsyncer()

	for {
		magic, err := binReader.ReadUInt32()
		c.logger.Debugf("read magic %0x", magic)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return binlogOpenError(fmt.Errorf("error reading magic: %w", err))
		}

		err = c.handler(magic, binReader)
		if err != nil {
			return binlogOpenError(fmt.Errorf("handler error: %w", err))
		}
	}

	return nil
}

func binlogOpenError(err error) error {
	return fmt.Errorf("binlog open error: %w", err)
}

func (c *Binlog) Write(magic uint32, data []byte) error {
	err := c.binWriter.writeUInt32(magic)
	if err != nil {
		return err
	}

	err = c.binWriter.writeBytes(data)
	if err != nil {
		return err
	}

	return nil
}

func (c *Binlog) fsyncer() {
	ticker := time.NewTicker(fsyncTime)

	c.logger.Infof("starting fsyncer")

	for range ticker.C {
		err := c.f.Sync()
		if err != nil {
			c.logger.Errorf("error calling fsync: %v", err)
		}
	}
}

func (c *Binlog) Close() error {
	err := c.binWriter.flush()
	if err != nil {
		return errClosingBinlog(fmt.Errorf("err flushing binwriter: %w", err))
	}

	err = c.f.Close()
	if err != nil {
		return errClosingBinlog(fmt.Errorf("err closing binlog file: %w", err))
	}

	return nil
}

func errClosingBinlog(err error) error {
	return fmt.Errorf("err closing binlog: %w", err)
}
