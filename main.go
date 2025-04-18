package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	perfKeyPrefix        = "__test"
	perfValueSizeBytes   = -1
	perfLargeValueSizeKB = -1
	perfNumThreads       = -1
	perfKeySpread        = -1
	perfSkip             = make([]string, 0)
	endpoints            = []string{"localhost:2379", "localhost:2380", "localhost:2381"}
	csvPath              = ""
	etcdClient           *clientv3.Client
	timeout              = 5 * time.Second
)

func init() {
	// Parse command line flags
	flag.IntVar(&perfValueSizeBytes, "value-size", 128, "The size of the value used for testing (in Bytes)")
	flag.IntVar(&perfLargeValueSizeKB, "large-value-size", 512, "How large the value for the set-large test should be (in KB)")
	flag.IntVar(&perfKeySpread, "keys", 100, "How many different keys to use for the tests")
	flag.IntVar(&perfNumThreads, "threads", 10, "Number of threads to use for the benchmark")
	flag.StringVar(&csvPath, "csv", "", "Optional path to save benchmark results as CSV")

	skipFlag := flag.String("skip", "", "Benchmarks to skip (comma separated - e.g. set,get)")
	endpointsFlag := flag.String("endpoints", "localhost:2379,localhost:2380,localhost:2381", "etcd endpoints (comma separated)")
	timeoutFlag := flag.Int("timeout", 5, "etcd operation timeout in seconds")

	flag.Parse()

	// Process flags
	if *skipFlag != "" {
		perfSkip = strings.Split(*skipFlag, ",")
	}

	if *endpointsFlag != "" {
		endpoints = strings.Split(*endpointsFlag, ",")
	}

	timeout = time.Duration(*timeoutFlag) * time.Second
}

func main() {
	fmt.Println("Performance testing tool for etcd servers")

	// Print configuration
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Printf("Endpoints: %s\n", strings.Join(endpoints, ", "))
	fmt.Printf("Timeout: %s\n", timeout)
	fmt.Printf("Threads: %d\n", perfNumThreads)
	fmt.Printf("Keys: %d\n", perfKeySpread)
	fmt.Printf("Large value size: %d KB\n", perfLargeValueSizeKB)
	if len(perfSkip) > 0 {
		fmt.Printf("Skipping tests: %s\n", strings.Join(perfSkip, ", "))
	}
	fmt.Println()

	// Linearizable Leseanfrage (stark konsistent)
	ctx := clientv3.WithRequireLeader(context.Background())

	// Create etcd client
	var err error
	etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	fmt.Println("Starting tests...")

	// Create results map
	results := make(map[string]testing.BenchmarkResult)

	// Run SET test
	setResult := testing.Benchmark(func(b *testing.B) {
		if shouldSkip("set") {
			return
		}

		// Prepare keys
		getKey, iter := getKeys("set")
		value := string(make([]byte, perfValueSizeBytes))

		// Cleanup
		b.Cleanup(func() {
			iter(func(k string) {
				_, err := etcdClient.Delete(ctx, k)
				if err != nil {
					log.Printf("(set) - error deleting key: %v\n", err)
				}
			})
		})

		b.SetParallelism(perfNumThreads)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := getKey(counter)
				_, err := etcdClient.Put(ctx, key, value)
				if err != nil {
					log.Printf("(set) - error setting key: %v\n", err)
				}
				counter++
			}
		})
	})

	results["set"] = setResult
	printResult("set", setResult)

	// Run SET-LARGE test
	setLargeValueResult := testing.Benchmark(func(b *testing.B) {
		if shouldSkip("set-large") {
			return
		}

		// Prepare keys
		getKey, iter := getKeys("set-large")
		// Prepare large value
		largeValue := string(make([]byte, perfLargeValueSizeKB*1024))

		// Cleanup
		b.Cleanup(func() {
			iter(func(k string) {
				_, err := etcdClient.Delete(ctx, k)
				if err != nil {
					log.Printf("(set-large) - error deleting key: %v\n", err)
				}
			})
		})

		b.SetParallelism(perfNumThreads)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := getKey(counter)
				_, err := etcdClient.Put(ctx, key, largeValue)
				if err != nil {
					log.Printf("(set-large) - error setting key: %v\n", err)
				}
				counter++
			}
		})
	})

	results["set-large"] = setLargeValueResult
	printResult("set-large", setLargeValueResult)

	// Run GET test
	getResult := testing.Benchmark(func(b *testing.B) {
		if shouldSkip("get") {
			return
		}

		// Prepare keys
		getKey, iter := getKeys("get")
		value := string(make([]byte, perfValueSizeBytes))

		// Set keys
		iter(func(k string) {
			_, err := etcdClient.Put(ctx, k, value)
			if err != nil {
				log.Printf("(get) - error setting key: %v\n", err)
			}
		})

		// Cleanup
		b.Cleanup(func() {
			iter(func(k string) {
				_, err := etcdClient.Delete(ctx, k)
				if err != nil {
					log.Printf("(get) - error deleting key: %v\n", err)
				}
			})
		})

		b.SetParallelism(perfNumThreads)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := getKey(counter)
				_, err := etcdClient.Get(ctx, key)
				if err != nil {
					log.Printf("(get) - error getting key: %v\n", err)
				}
				counter++
			}
		})
	})

	results["get"] = getResult
	printResult("get", getResult)

	// Run DELETE test
	deleteResult := testing.Benchmark(func(b *testing.B) {
		if shouldSkip("delete") {
			return
		}

		// Prepare keys
		getKey, iter := getKeys("delete")
		value := string(make([]byte, perfValueSizeBytes))

		// Set keys
		iter(func(k string) {
			_, err := etcdClient.Put(ctx, k, value)
			if err != nil {
				log.Printf("(delete) - error setting key: %v\n", err)
			}
		})

		// Cleanup fallback (for keys that might not have been deleted during the test)
		b.Cleanup(func() {
			iter(func(k string) {
				_, err := etcdClient.Delete(ctx, k)
				if err != nil {
					log.Printf("(delete cleanup) - error deleting key: %v\n", err)
				}
			})
		})

		b.SetParallelism(perfNumThreads)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := getKey(counter)
				_, err := etcdClient.Delete(ctx, key)
				if err != nil {
					log.Printf("(delete) - error deleting key: %v\n", err)
				}
				counter++
			}
		})
	})

	results["delete"] = deleteResult
	printResult("delete", deleteResult)

	// Run HAS test (implemented as a Get operation in etcd)
	hasResult := testing.Benchmark(func(b *testing.B) {
		if shouldSkip("has") {
			return
		}

		// Prepare keys
		getKey, iter := getKeys("has")
		value := string(make([]byte, perfValueSizeBytes))

		// Set keys
		iter(func(k string) {
			_, err := etcdClient.Put(ctx, k, value)
			if err != nil {
				log.Printf("(has) - error setting key: %v\n", err)
			}
		})

		// Cleanup
		b.Cleanup(func() {
			iter(func(k string) {
				_, err := etcdClient.Delete(ctx, k)
				if err != nil {
					log.Printf("(has) - error deleting key: %v\n", err)
				}
			})
		})

		b.SetParallelism(perfNumThreads)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := getKey(counter)
				resp, err := etcdClient.Get(ctx, key)
				if err != nil {
					log.Printf("(has) - error checking key: %v\n", err)
				} else {
					_ = len(resp.Kvs) > 0 // This is the equivalent of "has" operation
				}
				counter++
			}
		})
	})

	results["has"] = hasResult
	printResult("has", hasResult)

	// Run HAS-NOT test (checking for non-existent keys)
	hasNotResult := testing.Benchmark(func(b *testing.B) {
		if shouldSkip("has-not") {
			return
		}

		b.SetParallelism(perfNumThreads)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := fmt.Sprintf("%s/has-not-%d", perfKeyPrefix, counter%100)
				resp, err := etcdClient.Get(ctx, key)
				if err != nil {
					log.Printf("(has-not) - error checking key: %v\n", err)
				} else {
					_ = len(resp.Kvs) > 0 // This is the equivalent of "has" operation
				}
				counter++
			}
		})
	})

	results["has-not"] = hasNotResult
	printResult("has-not", hasNotResult)

	// Run MIXED test
	mixedUsageResult := testing.Benchmark(func(b *testing.B) {
		if shouldSkip("mixed") {
			return
		}

		// Prepare keys
		getKey, iter := getKeys("mixed")
		value := string(make([]byte, perfValueSizeBytes))

		// Set keys
		iter(func(k string) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			_, err := etcdClient.Put(ctx, k, value)
			cancel()
			if err != nil {
				log.Printf("(mixed) - error setting key: %v\n", err)
			}
		})

		// Cleanup
		b.Cleanup(func() {
			iter(func(k string) {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				_, err := etcdClient.Delete(ctx, k)
				cancel()
				if err != nil {
					log.Printf("(mixed) - error deleting key: %v\n", err)
				}
			})
		})

		b.SetParallelism(perfNumThreads)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := getKey(counter)
				ctx, cancel := context.WithTimeout(context.Background(), timeout)

				var err error
				switch counter % 5 {
				case 0: // set
					_, err = etcdClient.Put(ctx, key, value)
				case 1: // get
					_, err = etcdClient.Get(ctx, key)
				case 2: // delete
					_, err = etcdClient.Delete(ctx, key)
				case 3: // has (implemented as Get)
					resp, e := etcdClient.Get(ctx, key)
					err = e
					if err == nil {
						_ = len(resp.Kvs) > 0
					}
				case 4: // cas
					// Get then compare-and-swap
					getResp, e := etcdClient.Get(ctx, key)
					err = e
					if err == nil && len(getResp.Kvs) > 0 {
						txn := etcdClient.Txn(ctx).
							If(clientv3.Compare(clientv3.Value(key), "=", string(getResp.Kvs[0].Value))).
							Then(clientv3.OpPut(key, "new-value")).
							Else(clientv3.OpGet(key))
						_, err = txn.Commit()
					}
				}

				cancel()
				if err != nil {
					log.Printf("(mixed) - error performing operation (%d): %v\n", counter%5, err)
				}
				counter++
			}
		})
	})

	results["mixed"] = mixedUsageResult
	printResult("mixed", mixedUsageResult)

	// Write results to CSV if specified
	if csvPath != "" {
		fmt.Printf("\nExporting results to CSV: %s\n", csvPath)
		if err := writeResultsToCSV(csvPath, results); err != nil {
			log.Fatalf("Failed to export results to CSV: %v", err)
		}
		fmt.Println("Export complete")
	}
}

// --------------------------------------------------------------------------
// Helper Functions
// --------------------------------------------------------------------------

func shouldSkip(test string) bool {
	// Check if the test is in the skip list
	for _, skip := range perfSkip {
		if test == skip {
			return true
		}
	}
	return false
}

// Creates an array of test keys and functions to work with them
func getKeys(prefix string) (func(int) string, func(func(string))) {
	keys := make([]string, perfKeySpread)
	for i := 0; i < perfKeySpread; i++ {
		keys[i] = fmt.Sprintf("%s-%s-%d", perfKeyPrefix, prefix, i)
	}

	// Function to get a key by index (with wraparound)
	getKey := func(i int) string {
		return keys[i%perfKeySpread]
	}

	// Function to iterate over all keys and apply a function to each
	iterateKeys := func(fn func(string)) {
		for _, key := range keys {
			fn(key)
		}
	}

	return getKey, iterateKeys
}

// printResult prints the result of a benchmark test in a formatted way
func printResult(test string, result testing.BenchmarkResult) {
	if result.NsPerOp() == 0 {
		fmt.Printf("%-20sskipped\n", test)
		return
	}

	nsPerOp := math.Max(float64(result.NsPerOp()), 1) // prevent division by zero
	opsPerSec := 1.0 / (nsPerOp / 1e9)

	// Print the formatted result
	fmt.Printf("%-20s%.0fns/op (%s/op)\t%.0f ops/sec\n", test, nsPerOp, time.Duration(nsPerOp), opsPerSec)
}

// writeResultsToCSV writes benchmark results to a CSV file
func writeResultsToCSV(csvPath string, results map[string]testing.BenchmarkResult) error {
	file, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"Test", "NsPerOp", "DurationPerOp", "OpsPerSec", "Skipped",
		"Endpoints", "TimeoutSec", "Threads", "LargeValueSizeKB", "Keys Count",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %v", err)
	}

	// Write test results
	for test, result := range results {
		var nsPerOp float64
		var opsPerSec float64
		var skipped string

		if result.NsPerOp() == 0 {
			skipped = "true"
			nsPerOp = 0
			opsPerSec = 0
		} else {
			skipped = "false"
			nsPerOp = math.Max(float64(result.NsPerOp()), 1)
			opsPerSec = 1.0 / (nsPerOp / 1e9)
		}

		row := []string{
			test,
			fmt.Sprintf("%.0f", nsPerOp),
			time.Duration(nsPerOp).String(),
			fmt.Sprintf("%.0f", opsPerSec),
			skipped,
			strings.Join(endpoints, ";"),
			strconv.Itoa(int(timeout.Seconds())),
			strconv.Itoa(perfNumThreads),
			strconv.Itoa(perfLargeValueSizeKB),
			strconv.Itoa(perfKeySpread),
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row for test %s: %v", test, err)
		}
	}

	return nil
}
