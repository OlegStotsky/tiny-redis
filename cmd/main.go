package main

import (
	"flag"
	"go-redis/pkg"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	addrF := flag.String("addr", "localhost:3000", "addr to run server on")
	dbPathF := flag.String("db-path", "", "path to db")

	flag.Parse()

	logger, err := zap.NewDevelopment() // todo add log levels and use production
	if err != nil {
		log.Fatal(err.Error())
	}

	if *addrF == "" {
		logger.Fatal("addr can't be empty")
	}
	if *dbPathF == "" {
		logger.Fatal("db path cant be empty")
	}

	db, err := pkg.NewDB(*dbPathF)
	if err != nil {
		logger.Sugar().Fatalf("error opening db: %v", err)
	}

	if err := db.Open(); err != nil {
		logger.Sugar().Fatalf("error opening db: %v", err)
	}

	server := pkg.NewTinyRedisServer(*addrF, logger.Sugar(), db)

	go func() {
		if err := server.ListenAndServe(); err != nil {
			logger.Sugar().Errorf("error closing server: %v", err)
		}
	}()

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	<-exit

	logger.Sugar().Infof("shutting down...")

	if err := db.Close(); err != nil {
		logger.Sugar().Errorf("error closing db: %v", err)
	}
}
