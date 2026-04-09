package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if len(brokers) == 0 || brokers[0] == "" {
		brokers = []string{"localhost:9092"}
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl.AddConsumeTopics("engine_telemetry")

	count := 0
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			fmt.Printf("Errors: %v\n", errs)
			break
		}

		iter := fetches.RecordIter()
		if iter.Done() {
            if ctx.Err() != nil {
                break
            }
			continue
		}
		for !iter.Done() {
			iter.Next()
			count++
		}
        fmt.Printf("Current count: %d\n", count)
	}

	fmt.Printf("Total messages in topic: %d\n", count)
}
