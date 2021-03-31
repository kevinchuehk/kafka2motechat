package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type PassData struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Data  string `json:"data"`
}

const (
	topic         = "to-motechat"
	brokerAddress = "localhost:9092"
)

func main() {
	ctx := context.Background()
	consume(ctx)
}

func sendToHook(p PassData) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: tr,
	}

	resp, err := client.Post(
		`https://webhook.ypcloud.com/`+p.Name+`/`+p.Topic,
		`application/json`,
		strings.NewReader(p.Data),
	)

	if err != nil {
		log.Printf("Request Error: %s\n", err)
	}
	defer resp.Body.Close()
	log.Println("Request success")
}

func consume(ctx context.Context) {

	l := log.New(os.Stdout, "kafka reader: ", 0)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "consumer",
		Logger:  l,
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}

		var passdata PassData
		err = json.Unmarshal(msg.Value, &passdata)
		if err != nil {
			panic("could not parse message " + err.Error())
		}

		sendToHook(passdata)
	}
}