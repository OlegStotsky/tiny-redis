package pkg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	eventSetKeyMagic    = uint32(0xd1ab8645)
	eventDeleteKeyMagic = uint32(0x41f782f6)
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
		if _, err = os.Create(path); err != nil {
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

	logger.Debug("setting")

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

	if err := c.binlog.Write(eventSetKeyMagic, serializeSetEvent(key, val)); err != nil {
		return errSettingKey(err, key, val)
	}

	logger.Debug("set key to val success")

	return nil
}

func (c *DB) Delete(key string) (bool, error) {
	logger := c.logger.With(zap.String("key", key), zap.String("operation", "delete"))

	logger.Debug("deleting")

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.data[key]; ok {
		delete(c.data, key)

		if err := c.binlog.Write(eventDeleteKeyMagic, serializeDeleteEvent(key)); err != nil {
			return false, errDeletingKey(err, key)
		}

		logger.Debug("delete key success")
		return true, nil
	}

	return false, nil
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

func serializeDeleteEvent(key string) []byte {
	keyLen := len(key)

	b := make([]byte, 8+keyLen) // 8 bytes for key len followed by key itself

	binary.BigEndian.PutUint64(b[:8], uint64(keyLen))

	for i := 0; i < keyLen; i++ {
		b[8+i] = key[i]
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
			return dbBinlogHandlerError(errHandlingSet(fmt.Errorf("err reading key len : %w", err)))
		}

		valLen, err := rdr.ReadUInt64()
		if err != nil {
			return dbBinlogHandlerError(errHandlingSet(fmt.Errorf("err reading val len: %w", err)))
		}

		keyBuf := make([]byte, keyLen)
		err = rdr.ReadFull(keyBuf)
		if err != nil {
			return dbBinlogHandlerError(errHandlingSet(fmt.Errorf("err reading key: %w", err)))
		}

		valBuf := make([]byte, valLen)
		err = rdr.ReadFull(valBuf)
		if err != nil {
			return dbBinlogHandlerError(errHandlingSet(fmt.Errorf("err reading val: %w", err)))
		}

		keyStr := string(keyBuf)
		valStr := string(valBuf)

		c.data[keyStr] = &value{
			value: valStr,
		}

		c.logger.Debugf("successfully set key and val %s %s", keyStr, valStr)

	case eventDeleteKeyMagic:
		keyLen, err := rdr.ReadUInt64()
		if err != nil {
			return dbBinlogHandlerError(errHandlingDelete(fmt.Errorf("err reading key len: %w", err)))
		}

		keyBuf := make([]byte, keyLen)
		err = rdr.ReadFull(keyBuf)
		if err != nil {
			return dbBinlogHandlerError(errHandlingDelete(fmt.Errorf("err reading key: %w", err)))
		}

		keyStr := string(keyBuf)

		delete(c.data, keyStr)

		c.logger.Debug("successfully deleted key", keyStr)
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

func errDeletingKey(err error, key string) error {
	return fmt.Errorf("err deleting key %s: %w", key, err)
}

func errHandlingDelete(err error) error {
	return fmt.Errorf("err handling delete: %w", err)
}

func errHandlingSet(err error) error {
	return fmt.Errorf("err handling set: %w", err)
}

func dbBinlogHandlerError(err error) error {
	return fmt.Errorf("db binlog handler err: %w", err)
}
