package main

import (
	"flag"
	"go-redis/pkg"
	"go.uber.org/zap"
	"log"
)

func main() {
	addrF := flag.String("addr", "localhost:3000", "addr to run server on")

	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err.Error())
	}

	if *addrF == "" {
		logger.Fatal("addr can't be empty")
	}

	server := pkg.NewTinyRedisServer(*addrF, logger.Sugar())

	if err := server.ListenAndServe(); err != nil {
		logger.Sugar().Fatalf("error closing server: %v", err)
	}
}
