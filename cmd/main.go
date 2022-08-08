package main

import (
	"flag"
	"go-redis/pkg"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"

	"go.uber.org/zap"
)

func main() {
	addrF := flag.String("addr", "localhost:3000", "addr to run server on")
	dbPathF := flag.String("db-path", "", "path to db")

	flag.Parse()

	logger, err := zap.NewDevelopment() // todo add log levels and use production
	if err != nil {
		log.Fatal(err.Error())
	}

	dbPath := ""

	if *addrF == "" {
		logger.Fatal("addr can't be empty")
	}
	if *dbPathF == "" {
		logger.Warn("db path is empty, defaulting to cur dir")
		curDir, err := os.Getwd() //nolint:govet
		if err != nil {
			logger.Fatal("can't get current dir", zap.Error(err))
		}
		dbPath = path.Join(curDir, "default.db")
	} else {
		dbPath = *dbPathF
	}

	db, err := pkg.NewDB(dbPath)
	if err != nil {
		logger.Sugar().Fatalf("error opening db: %v", err)
	}

	if err = db.Open(); err != nil {
		logger.Sugar().Fatalf("error opening db: %v", err)
	}

	server := pkg.NewTinyRedisServer(*addrF, logger, db)

	go func() {
		if err = server.ListenAndServe(); err != nil {
			logger.Sugar().Errorf("error closing server: %v", err)
		}
	}()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	<-exit

	logger.Sugar().Infof("shutting down...")

	if err = db.Close(); err != nil {
		logger.Sugar().Errorf("error closing db: %v", err)
	}
}
