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

queries:
	wget https://s3.amazonaws.com/benchmarks.redislabs/redisgraph/datasets/k-hop/graph500_s22_nodes.csv
	python3 scripts/generate_graph500_queries.py --nodes-file graph500_s22_nodes.csv
