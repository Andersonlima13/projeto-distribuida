package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"pubsub/metrics"
	"pubsub/scenarios"
)

func main() {
	listOnly := flag.Bool("list", false, "list available scenarios")
	scenarioName := flag.String("scenario", "steady-fanout", "scenario name")
	jsonPath := flag.String("json", "", "output JSON report path")
	csvPath := flag.String("csv", "", "output CSV report path")
	flag.Parse()

	registry := scenarios.NewRegistry()
	if *listOnly {
		names := registry.Names()
		sort.Strings(names)
		for _, name := range names {
			fmt.Println(name)
		}
		return
	}

	scenario, exists := registry.Get(*scenarioName)
	if !exists {
		log.Fatalf("unknown scenario %q", *scenarioName)
	}
	report, err := scenario.Run()
	if err != nil {
		log.Fatalf("run %s: %v", *scenarioName, err)
	}
	fmt.Printf("scenario=%s p50_ms=%.3f p99_ms=%.3f p99_9_ms=%.3f throughput_pub=%d throughput_delivered=%d overflow=%d failover=%d network=%d throttled=%d jain=%.6f\n",
		*scenarioName, report.P50Millis, report.P99Millis, report.P999Millis,
		report.Published, report.Delivered, report.OverflowLoss, report.FailoverLoss,
		report.NetworkLoss, report.Throttled, report.JainIndex)

	if *jsonPath != "" {
		if err := ensureParent(*jsonPath); err != nil {
			log.Fatal(err)
		}
		if err := metrics.WriteJSON(*jsonPath, report); err != nil {
			log.Fatalf("write JSON: %v", err)
		}
	}
	if *csvPath != "" {
		if err := ensureParent(*csvPath); err != nil {
			log.Fatal(err)
		}
		if err := metrics.WriteCSV(*csvPath, report); err != nil {
			log.Fatalf("write CSV: %v", err)
		}
	}
}

func ensureParent(path string) error {
	lastSlash := -1
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			lastSlash = i
			break
		}
	}
	if lastSlash < 0 {
		return nil
	}
	return os.MkdirAll(path[:lastSlash], 0755)
}
