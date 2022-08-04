package pkg

import (
	"fmt"
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	commandPing = "ping"
	commandQuit = "quit"
	commandSet  = "set"
	commandGet  = "get"
	commandDel  = "del"
)

type TinyRedisServer struct {
	addr   string
	db     *DB
	mu     sync.RWMutex
	logger *zap.SugaredLogger
}

func NewTinyRedisServer(addr string, logger *zap.SugaredLogger, db *DB) *TinyRedisServer {
	return &TinyRedisServer{
		addr:   addr,
		logger: logger,
		db:     db,
	}
}

func (c *TinyRedisServer) ListenAndServe() error {
	c.logger.Infof("listening on %v", c.addr)

	return redcon.ListenAndServe(c.addr, c.handler, c.accepter, c.closer)
}

func (c *TinyRedisServer) handler(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	case commandPing:
		c.pingHandler(conn, cmd)
	case commandQuit:
		c.quitHandler(conn, cmd)
	case commandSet:
		c.setHandler(conn, cmd)
	case commandGet:
		c.getHandler(conn, cmd)
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	}
}

func (c *TinyRedisServer) pingHandler(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (c *TinyRedisServer) quitHandler(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
}

type setOptions struct {
	nx bool // set only if already exists
	xx bool // set only if doesn't exist

	ttl time.Time

	// todo keepttl
}

func (c *setOptions) String() string {
	return "nx " + strconv.FormatBool(c.nx) + " " + "xx " + strconv.FormatBool(c.xx) + " " + "ttl " + c.ttl.String()
}

func (c *TinyRedisServer) setHandler(conn redcon.Conn, cmd redcon.Command) {
	c.logger.Debug("running set with", zap.String("key", string(cmd.Args[1])), zap.String("val", string(cmd.Args[2])))

	c.mu.Lock()
	defer c.mu.Unlock()

	key := cmd.Args[1]
	val := cmd.Args[2]

	options, err := parseSetOptions(cmd)
	if err != nil {
		c.logger.Debug("error in set handler", zap.Error(err))
		conn.WriteError(err.Error())
		return
	}

	err = c.db.Set(string(key), string(val), &options)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteString("OK") // todo should we write okay if we didn't set?
}

func parseSetOptions(cmd redcon.Command) (setOptions, error) {
	options := setOptions{}

	if len(cmd.Args) <= 3 {
		return options, nil
	}

	for idx, kk := range cmd.Args[3:] {
		switch string(kk) {
		case "NX":
			options.nx = true
		case "XX":
			options.xx = true
		case "EX":
			if idx+1 >= len(cmd.Args) {
				return setOptions{}, fmt.Errorf("error parsing SET command: missing argument for EX")
			}
			secondsRaw := cmd.Args[idx+3+1]
			seconds, err := strconv.ParseInt(string(secondsRaw), 10, 64)
			if err != nil {
				return setOptions{}, errParsingSetCommand(err)
			}
			options.ttl = time.Now().Add(time.Duration(seconds) * time.Second)
		case "PX":
			if idx+1 >= len(cmd.Args) {
				return setOptions{}, fmt.Errorf("error parsing SET command: missing argument for PX")
			}
			millisecondsRaw := cmd.Args[idx+3+1]
			milliseconds, err := strconv.ParseInt(string(millisecondsRaw), 10, 64)
			if err != nil {
				return setOptions{}, errParsingSetCommand(err)
			}
			options.ttl = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
		case "EXAT":
			if idx+1 >= len(cmd.Args) {
				return setOptions{}, fmt.Errorf("error parsing SET command: missing argument for EXAT")
			}
			secondsRaw := cmd.Args[idx+3+1]
			seconds, err := strconv.ParseInt(string(secondsRaw), 10, 64)
			if err != nil {
				return setOptions{}, errParsingSetCommand(err)
			}
			options.ttl = time.Unix(seconds, 0)
		case "PXAT":
			if idx+1 >= len(cmd.Args) {
				return setOptions{}, fmt.Errorf("error parsing SET command: missing argument for PXAT")
			}
			millisecondsRaw := cmd.Args[idx+3+1]
			milliseconds, err := strconv.ParseInt(string(millisecondsRaw), 10, 64)
			if err != nil {
				return setOptions{}, errParsingSetCommand(err)
			}
			options.ttl = time.Unix(milliseconds/1000, 0)
		}
	}

	return options, nil
}

func errParsingSetCommand(err error) error {
	return fmt.Errorf("error parsing SET command: %w", err)
}

func (c *TinyRedisServer) getHandler(conn redcon.Conn, cmd redcon.Command) {
	c.logger.Debugf("running get with k %v", cmd.Args[1])

	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	key := cmd.Args[1]

	val, ok := c.db.Get(string(key))
	if ok {
		conn.WriteBulk([]byte(val))
	} else {
		conn.WriteNull()
	}
}

func (c *TinyRedisServer) accepter(_ redcon.Conn) bool {
	return true
}

func (c *TinyRedisServer) closer(_ redcon.Conn, _ error) {
}
