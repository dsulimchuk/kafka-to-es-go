package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/segmentio/kafka-go"

	_ "github.com/segmentio/kafka-go/gzip"
)

func main() {
	fmt.Println("hello world")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"docker01.dev.mdlp.crpt.tech:9092"},
		GroupID:        "consumer-group-id3",
		Topic:          "kiz-filter-topic",
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		QueueCapacity:  1000,
		CommitInterval: time.Second,
	})
	ctx := context.Background()
	es := es()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Println(err)
			break
		}
		var result map[string]interface{}
		json.Unmarshal(m.Value, &result)
		id := result["sgtin"].(string)
		//fmt.Println(result, id)
		//fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %d\n", m.Topic, m.Partition, m.Offset, string(m.Key), len(m.Value))
		saveToEs(es, id, string(m.Value))
		r.CommitMessages(ctx, m)
	}

	r.Close()
}

func es() *elasticsearch.Client {
	cfg := elasticsearch.Config{
		Addresses: []string{"http://docker01.dev.mdlp.crpt.tech:9200"},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	return es
}
func saveToEs(es *elasticsearch.Client, id string, value string) {

	req := esapi.IndexRequest{
		Index:      "hello",
		DocumentID: id,
		Body:       strings.NewReader(value),
	}
	res1, err1 := req.Do(context.Background(), es)
	if err1 != nil {
		log.Fatalf("Error getting response: %s", err1)
	}
	defer res1.Body.Close()
	println("maybe save")
}
