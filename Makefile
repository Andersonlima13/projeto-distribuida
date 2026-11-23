GO ?= go

.PHONY: test build bench demo broker clean

test:
	$(GO) test ./...

build:
	$(GO) build ./...

bench:
	mkdir -p results
	$(GO) run ./cmd/bench -scenario steady-fanout -json results/steady-fanout.json -csv results/steady-fanout.csv
	$(GO) run ./cmd/bench -scenario burst-publisher -json results/burst-publisher.json -csv results/burst-publisher.csv
	$(GO) run ./cmd/bench -scenario slow-subscriber -json results/slow-subscriber.json -csv results/slow-subscriber.csv
	$(GO) run ./cmd/bench -scenario primary-failover -json results/primary-failover.json -csv results/primary-failover.csv

broker:
	$(GO) run ./cmd/broker -address 127.0.0.1:9000

demo:
	$(GO) run ./cmd/demo -broker 127.0.0.1:9000 -client demo-client

clean:
	rm -rf bin
