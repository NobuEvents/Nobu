package kafka

import (
	"context"
	"fmt"
	"github.com/NobuEvents/Nobu/pkg/events"
	"github.com/NobuEvents/Nobu/pkg/router"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
)

type KafkaClusterConfig struct {
	seed_brokers   []string
	consumer_group string // optional - only needed for Kafka consumers
	topic          string
}

type KafkaWriter struct {
	KafkaClusterConfig
	client *kgo.Client
	routes *router.Routes
}

func NewKafkaWriter(config KafkaClusterConfig) (*KafkaWriter, error) {
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(config.seed_brokers...),
	)

	if err != nil {
		return nil, fmt.Errorf("kafka Client failed: %w", err)
	}

	return &KafkaWriter{client: kafkaClient, KafkaClusterConfig: config, routes: router.NewRoutes()}, nil
}

func (writer *KafkaWriter) Dispath(event events.Event) {
	route := writer.routes.NobuTypeToTargetRoute(event.Type)
	var wg sync.WaitGroup
	ctx := context.Background()
	record := &kgo.Record{
		Key:     []byte(""), // we need to capture the Key of the message too.
		Topic:   route.Topic,
		Value:   event.Message,
		Headers: []kgo.RecordHeader{}, // How to handle the header
	}
	writer.client.Produce(ctx, record, func(record *kgo.Record, err error) {
		defer wg.Done()

		if err != nil {
			// Add logging trace for the failed message
		}

	})
	wg.Wait()
}
