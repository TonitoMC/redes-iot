// Producer: simula el nodo de sensores y publica telemetría al broker Kafka.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/segmentio/kafka-go"

	"meteo/internal/kafkacfg"
	"meteo/internal/sensor"
)

func main() {
	broker := flag.String("broker", kafkacfg.DefaultBroker, "bootstrap server")
	topic := flag.String("topic", "201105", "topic (carné de un integrante)")
	minS := flag.Float64("min", 15, "intervalo mínimo entre envíos (s)")
	maxS := flag.Float64("max", 30, "intervalo máximo entre envíos (s)")
	flag.Parse()
	if *topic == "" {
		log.Fatal("falta -topic <carné>")
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(*broker),
		Topic:                  *topic,
		Balancer:               &kafka.Hash{}, // misma key -> misma partición
		Transport:              kafkacfg.Transport(),
		AllowAutoTopicCreation: true,
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Printf("producer -> %s topic=%s", *broker, *topic)
	for {
		r := sensor.Generate()
		payload, _ := json.Marshal(r)

		err := w.WriteMessages(ctx, kafka.Message{Key: []byte("sensor1"), Value: payload})
		if ctx.Err() != nil {
			log.Println("detenido")
			return
		}
		if err != nil {
			log.Printf("error enviando: %v", err)
		} else {
			log.Printf("enviado %s", payload)
		}

		wait := time.Duration((*minS + rand.Float64()*(*maxS-*minS)) * float64(time.Second))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			log.Println("detenido")
			return
		}
	}
}
