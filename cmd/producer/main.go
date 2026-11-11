// Producer: nodo de sensores simulado de la estación meteorológica.
//
// Genera una lectura de temperatura, humedad y dirección del viento cada 15 a
// 30 segundos (intervalo aleatorio) y la publica como JSON en un topic del
// broker Kafka. Corre hasta que se interrumpe con Ctrl+C.
//
// Todos los mensajes se envían con la key "sensor1". Kafka guarda en la misma
// partición los mensajes con la misma key, así que las lecturas de este nodo
// se conservan en el orden en que se enviaron.
//
// El topic por defecto es el carné de un integrante del grupo, para no mezclar
// datos con otras parejas. Uso:
//
//	go run ./cmd/producer [-topic 201105] [-broker host:9092] [-min 15] [-max 30]
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

// Main configura el Writer y envía lecturas en un ciclo hasta recibir Ctrl+C.
func main() {
	broker := flag.String("broker", kafkacfg.DefaultBroker, "bootstrap server")
	topic := flag.String("topic", "201105", "topic (carné de un integrante)")
	minS := flag.Float64("min", 15, "intervalo mínimo entre envíos (s)")
	maxS := flag.Float64("max", 30, "intervalo máximo entre envíos (s)")
	flag.Parse()
	if *topic == "" {
		log.Fatal("el topic no puede estar vacío")
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(*broker),
		Topic:                  *topic,
		Balancer:               &kafka.Hash{}, // Misma key -> misma partición
		Transport:              kafkacfg.Transport(),
		AllowAutoTopicCreation: true,
	}
	defer w.Close()

	// Cancelar el contexto al recibir Ctrl+C
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Printf("producer -> %s topic=%s", *broker, *topic)
	for {
		payload, _ := json.Marshal(sensor.Generate())

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

		// Esperar un intervalo aleatorio entre min y max segundos
		wait := time.Duration((*minS + rand.Float64()*(*maxS-*minS)) * float64(time.Second))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			log.Println("detenido")
			return
		}
	}
}
