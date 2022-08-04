package pkg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

const (
	eventSetKeyMagic = uint32(0x123)
)

type value struct {
	value string

	ttl time.Time
}

func (c *value) expired() bool {
	return c.ttl != time.Time{} && time.Now().After(c.ttl)
}

type DB struct {
	binlog *Binlog

	mu   sync.RWMutex
	data map[string]*value

	logger *zap.SugaredLogger
}

func NewDB(path string) (*DB, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if _, err := os.Create(path); err != nil {
			return nil, errCreatingDB(err)
		}
	}

	db := DB{data: map[string]*value{}}
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, errCreatingDB(err)
	}
	suggaredLogger := logger.Sugar()
	binlog, err := NewBinlog(path, db.binlogHandler, suggaredLogger)
	if err != nil {
		return nil, errCreatingDB(err)
	}
	db.binlog = binlog
	db.logger = suggaredLogger

	return &db, nil
}

func (c *DB) Open() error {
	c.logger.Infof("opening db...")

	err := c.binlog.Open()
	if err != nil {
		return errOpeningDB(err)
	}

	return nil
}

func (c *DB) Set(key string, val string, setOptions *setOptions) error {
	logger := c.logger.With(zap.String("key", key), zap.String("val", val), zap.String("options", setOptions.String()))

	logger.Debugf("setting")

	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.data[key]
	if setOptions.nx && ok {
		logger.Debug("setting key to val with nx option but key already exists")
		return nil
	}
	if setOptions.xx && !ok {
		logger.Debug("setting key to val with xx option but key doesn't exists")
		return nil
	}

	c.data[key] = &value{
		value: val,
		ttl:   setOptions.ttl,
	}

	err := c.binlog.Write(eventSetKeyMagic, serializeSetEvent(key, val))
	if err != nil {
		return errSettingKey(err, key, val)
	}

	logger.Debugf("set key to val success")

	return nil
}

func serializeSetEvent(key string, value string) []byte {
	keyLen := len(key)
	valLen := len(value)

	b := make([]byte, 16+keyLen+valLen)

	binary.BigEndian.PutUint64(b[:8], uint64(keyLen))
	binary.BigEndian.PutUint64(b[8:16], uint64(valLen))

	for i := 0; i < keyLen; i++ {
		b[16+i] = key[i]
	}

	for i := 0; i < valLen; i++ {
		b[16+keyLen+i] = value[i]
	}

	return b
}

func (c *DB) Get(key string) (string, bool) {
	c.logger.Debugf("getting key %v", key)

	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.data[key]
	if !ok {
		return "", false
	}
	if item.expired() {
		return "", false
	}

	return item.value, true
}

func (c *DB) Close() error {
	c.logger.Infof("closing db...")

	err := c.binlog.Close()
	if err != nil {
		return fmt.Errorf("err closing db: %w", err)
	}

	return nil
}

func (c *DB) binlogHandler(magic uint32, rdr *binlogReader) error {
	c.logger.Debugf("running handler for magic %0x", magic)

	switch magic {
	case eventSetKeyMagic:
		keyLen, err := rdr.ReadUInt64()
		if err != nil {
			return dbBinlogHandlerError(fmt.Errorf("err reading key len : %w", err))
		}

		valLen, err := rdr.ReadUInt64()
		if err != nil {
			return dbBinlogHandlerError(fmt.Errorf("err reading val len: %w", err))
		}

		keyBuf := make([]byte, keyLen)
		err = rdr.ReadFull(keyBuf)
		if err != nil {
			return dbBinlogHandlerError(fmt.Errorf("error reading key: %w", err))
		}

		valBuf := make([]byte, valLen)
		err = rdr.ReadFull(valBuf)
		if err != nil {
			return dbBinlogHandlerError(fmt.Errorf("error reading val: %w", err))
		}

		keyStr := string(keyBuf)
		valStr := string(valBuf)

		c.data[keyStr] = &value{
			value: valStr,
		}

		c.logger.Debugf("successfully read key and val %s %s", keyStr, valStr)
	}

	return nil
}

func errCreatingDB(err error) error {
	return fmt.Errorf("err creating db: %w", err)
}

func errOpeningDB(err error) error {
	return fmt.Errorf("err opening db: %w", err)
}

func errSettingKey(err error, key string, value string) error {
	return fmt.Errorf("err setting key %s to value %s: %w", key, value, err)
}

func dbBinlogHandlerError(err error) error {
	return fmt.Errorf("db binlog handler err: %w", err)
}
