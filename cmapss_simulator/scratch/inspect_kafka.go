package main

import (
	"context"
	"fmt"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumeTopics("engine_telemetry"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	fetches := client.PollRecords(ctx, 1)
	if fetches.IsClientClosed() {
		return
	}
	
	records := fetches.Records()
	if len(records) > 0 {
		fmt.Printf("%s\n", records[0].Value)
	}
}
