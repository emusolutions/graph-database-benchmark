# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

.PHONY: all benchmark
all: get test benchmark

benchmark: redisgraph-database-benchmark

redisgraph-database-benchmark:
	go build -o bin/$@ ./$@
	go install ./$@

get:
	go get ./...

test: get
	go fmt ./...
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...
