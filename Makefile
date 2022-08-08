build:
	go build -o ./bin/server ./cmd/main.go

test:
	go test ./...

run: build
	./bin/server

lint:
	golangci-lint run
