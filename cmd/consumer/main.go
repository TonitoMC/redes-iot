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
// Cada lectura recibida también se guarda en un archivo CSV (-out), como
// respaldo de los datos por si el broker deja de estar disponible. El archivo
// se reescribe en cada ejecución: como siempre se lee todo el historial del
// topic, no se duplican filas.
//
// Se lee desde el primer offset del topic, así que las gráficas incluyen la
// telemetría pasada además de la que va llegando. Cada ejecución usa un
// consumer group nuevo por defecto para que siempre se lea todo el historial.
//
// Uso:
//
//	go run ./cmd/consumer [-topic 201105] [-broker host:9092] [-http :8080] [-group id] [-out lecturas.csv] [-compact]
//
// Sin acceso al broker, -replay lecturas.csv repite las lecturas guardadas
// pasándolas por el codec de 3 bytes y las grafica igual.
//
// Abrir http://localhost:8080 en el navegador.
package main

import (
	"context"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"meteo/internal/codec"
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
	out := flag.String("out", "lecturas.csv", "archivo CSV donde se guardan las lecturas (vacío para desactivar)")
	compact := flag.Bool("compact", false, "decodificar el payload de 3 bytes en lugar de JSON")
	replay := flag.String("replay", "", "CSV de lecturas a repetir sin conectarse al broker (codifica y decodifica en 3 bytes)")
	delay := flag.Duration("replay-delay", 300*time.Millisecond, "pausa entre lecturas del replay")
	flag.Parse()
	if *topic == "" {
		log.Fatal("el topic no puede estar vacío")
	}

	// Cancelar el contexto al recibir Ctrl+C
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	source := fmt.Sprintf("%s topic=%s", *broker, *topic)
	if *replay != "" {
		// Sin broker: se repiten las lecturas del CSV pasándolas por el codec
		source = "replay de " + *replay
		go replayCSV(ctx, *replay, *delay)
	} else {
		// Leer desde el primer mensaje para incluir el historial
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{*broker},
			Topic:       *topic,
			GroupID:     *group,
			StartOffset: kafka.FirstOffset,
			Dialer:      kafkacfg.Dialer(),
		})
		defer r.Close()

		// Abrir el CSV de respaldo (se reescribe en cada ejecución)
		var w *csv.Writer
		if *out != "" {
			f, err := os.Create(*out)
			if err != nil {
				log.Fatalf("no se pudo crear %s: %v", *out, err)
			}
			defer f.Close()
			w = csv.NewWriter(f)
			w.Write([]string{"timestamp", "particion", "offset", "temperatura", "humedad", "direccion_viento"})
			w.Flush()
			log.Printf("guardando lecturas en %s", *out)
		}

		go consume(ctx, r, w, *compact)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveDashboard)
	mux.HandleFunc("/api/data", serveData)
	srv := &http.Server{Addr: *addr, Handler: mux}

	// Apagar el servidor al recibir Ctrl+C
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	log.Printf("consumer <- %s | dashboard en http://localhost%s", source, *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// ReplayCSV repite sin broker las lecturas de un CSV generado por -out. Cada
// lectura se codifica a 3 bytes y se decodifica de vuelta, como ocurriría
// entre el producer y el consumer con -compact, y se agrega a points. Al
// final informa si alguna lectura cambió en el viaje de ida y vuelta.
func replayCSV(ctx context.Context, path string, delay time.Duration) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("no se pudo abrir %s: %v", path, err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		log.Fatalf("CSV inválido: %v", err)
	}
	if len(rows) < 2 {
		log.Fatalf("%s no tiene lecturas", path)
	}

	ok, bad := 0, 0
	for _, row := range rows[1:] { // La primera fila es el encabezado
		if len(row) < 6 {
			continue
		}
		t, err1 := time.Parse(time.RFC3339, row[0])
		temp, err2 := strconv.ParseFloat(row[3], 64)
		hum, err3 := strconv.Atoi(row[4])
		if err1 != nil || err2 != nil || err3 != nil {
			log.Printf("fila inválida %v", row)
			continue
		}

		in := sensor.Reading{Temperatura: temp, Humedad: hum, DireccionViento: row[5]}
		payload, err := codec.Encode(in)
		if err != nil {
			log.Printf("no se pudo codificar %+v: %v", in, err)
			bad++
			continue
		}
		out, err := codec.Decode(payload)
		if err != nil || out != in {
			log.Printf("ida y vuelta distinta: %+v -> %x -> %+v (%v)", in, payload, out, err)
			bad++
			continue
		}
		ok++
		log.Printf("%+v -> %x (%d bytes) -> %+v", in, payload, len(payload), out)

		mu.Lock()
		points = append(points, point{
			Time:            t.Local().Format("15:04:05"),
			Temperatura:     out.Temperatura,
			Humedad:         out.Humedad,
			DireccionViento: out.DireccionViento,
		})
		mu.Unlock()

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
	}
	log.Printf("replay terminado: %d lecturas iguales tras codificar y decodificar, %d con diferencias", ok, bad)
}

// Consume lee mensajes de Kafka hasta que se cancela ctx. Cada mensaje se
// decodifica de JSON y se agrega a points. Los mensajes inválidos se ignoran
// y los errores de lectura se reintentan tras una pausa. Si w no es nil, cada
// lectura válida también se escribe como una fila del CSV.
func consume(ctx context.Context, r *kafka.Reader, w *csv.Writer, compact bool) {
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
		if compact {
			if rd, err = codec.Decode(m.Value); err != nil {
				log.Printf("mensaje inválido %x: %v", m.Value, err)
				continue
			}
			log.Printf("recibido p%d@%d %x (%d bytes) -> %+v", m.Partition, m.Offset, m.Value, len(m.Value), rd)
		} else {
			if err := json.Unmarshal(m.Value, &rd); err != nil {
				log.Printf("mensaje inválido %q: %v", m.Value, err)
				continue
			}
			log.Printf("recibido p%d@%d %s", m.Partition, m.Offset, m.Value)
		}

		// Guardar la lectura con la hora en que se publicó
		mu.Lock()
		points = append(points, point{
			Time:            m.Time.Local().Format("15:04:05"),
			Temperatura:     rd.Temperatura,
			Humedad:         rd.Humedad,
			DireccionViento: rd.DireccionViento,
		})
		mu.Unlock()

		// Respaldo en CSV, con flush por fila para no perder datos al cerrar
		if w != nil {
			w.Write([]string{
				m.Time.Format(time.RFC3339),
				strconv.Itoa(m.Partition),
				strconv.FormatInt(m.Offset, 10),
				strconv.FormatFloat(rd.Temperatura, 'f', 2, 64),
				strconv.Itoa(rd.Humedad),
				rd.DireccionViento,
			})
			w.Flush()
		}
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
