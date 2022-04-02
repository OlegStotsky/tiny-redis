package pkg

import (
	"github.com/tidwall/redcon"
	"go.uber.org/zap"
	"strings"
	"sync"
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

func (c *TinyRedisServer) setHandler(conn redcon.Conn, cmd redcon.Command) {
	c.logger.Debugf("running set with k %v and s %v other args %v", cmd.Args[1], cmd.Args[2], cmd.Args[2:])

	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := cmd.Args[1]
	val := cmd.Args[2]

	err := c.db.Set(string(key), string(val))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteString("OK")
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

	val, err := c.db.Get(string(key))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	if val != "" {
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
