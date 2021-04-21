package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-resty/resty/v2"
	"github.com/segmentio/kafka-go"
)

type PassData struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Data  string `json:"data"`
}

type AuthSuccess struct {
	/* variables */
}

const (
	topic          = "to-motechat"
	defaultAddress = "localhost:9092"
)

var brokerAddress = os.Getenv("BROKER_ADDRESS")

func main() {
	ctx := context.Background()
	consume(ctx)
}

func sendToHook(p *PassData) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := resty.New()
	client.SetTransport(tr)
	_, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(p.Data).
		SetResult(AuthSuccess{}).
		Post(`https://webhook.ypcloud.com/` + p.Name + `/` + p.Topic)

	if err != nil {
		log.Printf("Request Error: %s\n", err)
	}
}

func consume(ctx context.Context) {
	addr := brokerAddress
	if addr == "" {
		addr = defaultAddress
	}
	fmt.Printf("BROKER_ADDRESS: %s\n", addr)
	l := log.New(os.Stdout, "kafka reader: ", 0)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{addr},
		Topic:   topic,
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

		sendToHook(&passdata)
	}
}
