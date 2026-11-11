// Consumer: lee la telemetría del topic y la grafica en vivo.
//
// El programa hace dos cosas al mismo tiempo:
//
//  1. Una goroutine lee los mensajes del topic en Kafka, los decodifica de
//     JSON y los guarda en memoria.
//  2. Un servidor HTTP entrega el dashboard (dashboard.html, incluido en el
//     binario con go:embed) y los datos acumulados en /api/data.
//
// El dashboard consulta /api/data cada pocos segundos y redibuja las gráficas
// de temperatura, humedad y dirección del viento con Chart.js.
//
// Se lee desde el primer offset del topic, así que las gráficas incluyen la
// telemetría pasada además de la que va llegando. Cada ejecución usa un
// consumer group nuevo por defecto para que siempre se lea todo el historial.
//
// Uso:
//
//	go run ./cmd/consumer [-topic 201105] [-broker host:9092] [-http :8080] [-group id]
//
// Abrir http://localhost:8080 en el navegador.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"meteo/internal/kafkacfg"
	"meteo/internal/sensor"
)

// DashboardHTML es la página del dashboard, incluida en el binario al compilar.
//
//go:embed dashboard.html
var dashboardHTML []byte

// Point es una lectura ya procesada, tal como la consume el dashboard.
type point struct {
	Time            string  `json:"t"` // Hora de la lectura (HH:MM:SS)
	Temperatura     float64 `json:"temperatura"`
	Humedad         int     `json:"humedad"`
	DireccionViento string  `json:"direccion_viento"`
}

var (
	mu     sync.RWMutex // Protege points: lo escribe el consumidor y lo lee el servidor HTTP
	points = []point{}  // Todas las lecturas recibidas
)

// Main inicia el consumidor de Kafka y el servidor del dashboard.
func main() {
	broker := flag.String("broker", kafkacfg.DefaultBroker, "bootstrap server")
	topic := flag.String("topic", "201105", "topic (carné de un integrante)")
	group := flag.String("group", fmt.Sprintf("meteo-dashboard-%d", time.Now().Unix()), "consumer group id")
	addr := flag.String("http", ":8080", "dirección del dashboard")
	flag.Parse()
	if *topic == "" {
		log.Fatal("el topic no puede estar vacío")
	}

	// Cancelar el contexto al recibir Ctrl+C
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Leer desde el primer mensaje para incluir el historial
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{*broker},
		Topic:       *topic,
		GroupID:     *group,
		StartOffset: kafka.FirstOffset,
		Dialer:      kafkacfg.Dialer(),
	})
	defer r.Close()

	go consume(ctx, r)

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveDashboard)
	mux.HandleFunc("/api/data", serveData)
	srv := &http.Server{Addr: *addr, Handler: mux}

	// Apagar el servidor al recibir Ctrl+C
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	log.Printf("consumer <- %s topic=%s | dashboard en http://localhost%s", *broker, *topic, *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// Consume lee mensajes de Kafka hasta que se cancela ctx. Cada mensaje se
// decodifica de JSON y se agrega a points. Los mensajes inválidos se ignoran
// y los errores de lectura se reintentan tras una pausa.
func consume(ctx context.Context, r *kafka.Reader) {
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("error leyendo: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		var rd sensor.Reading
		if err := json.Unmarshal(m.Value, &rd); err != nil {
			log.Printf("mensaje inválido %q: %v", m.Value, err)
			continue
		}
		log.Printf("recibido p%d@%d %s", m.Partition, m.Offset, m.Value)

		// Guardar la lectura con la hora en que se publicó
		mu.Lock()
		points = append(points, point{
			Time:            m.Time.Local().Format("15:04:05"),
			Temperatura:     rd.Temperatura,
			Humedad:         rd.Humedad,
			DireccionViento: rd.DireccionViento,
		})
		mu.Unlock()
	}
}

// ServeDashboard responde con la página HTML del dashboard.
func serveDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(dashboardHTML)
}

// ServeData responde con todas las lecturas recibidas, en formato JSON.
func serveData(w http.ResponseWriter, _ *http.Request) {
	mu.RLock()
	defer mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(points)
}
