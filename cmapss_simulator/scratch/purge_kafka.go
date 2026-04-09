package main

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	ctx := context.Background()

	_, err = adm.DeleteTopics(ctx, "engine_telemetry")
	if err != nil {
		log.Printf("Warning: topic deletion failed (might not exist): %v", err)
	}

	_, err = adm.CreateTopics(ctx, 1, 1, nil, "engine_telemetry")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Topic engine_telemetry purged!")
}
