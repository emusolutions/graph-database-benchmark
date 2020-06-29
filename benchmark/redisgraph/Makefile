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

redisgraph-database-benchmark: test
	go build -o bin/$@ ./$@
	go install ./$@

get:
	go get ./...

test: get
	go fmt ./...
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...

dataset_docker:
	wget https://s3.amazonaws.com/benchmarks.redislabs/redisgraph/datasets/k-hop/graph500_s22_dump.rdb
	mv graph500_s22_dump.rdb dump.rdb
	docker run --volume="$PWD/":/data/:rw -p 6379:6379 redislabs/redisgraph:edge  redis-server --loadmodule /usr/lib/redis/modules/redisgraph.so


