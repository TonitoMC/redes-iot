// Package kafkacfg centraliza los datos de conexión y autenticación al
// broker de Kafka, para que el productor y el consumidor los compartan.
//
// El broker del laboratorio usa SASL_PLAINTEXT con el mecanismo PLAIN:
// usuario y contraseña viajan sin cifrar sobre TCP (puerto 9092). Es
// suficiente para la actividad, pero no se debería usar así en producción.
//
// kafka-go necesita la autenticación en dos lugares distintos:
//   - Transport, para el kafka.Writer (productor).
//   - Dialer, para el kafka.Reader (consumidor).
package kafkacfg

import (
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	// DefaultBroker es el bootstrap server del laboratorio (IP:puerto).
	DefaultBroker = "161.35.11.141:9092"

	username = "estudiante"
	password = "TryluRandomPass-2!@!"
)

// Mechanism devuelve el mecanismo SASL PLAIN con las credenciales del laboratorio.
func mechanism() plain.Mechanism {
	return plain.Mechanism{Username: username, Password: password}
}

// Transport devuelve el transporte autenticado que usa el kafka.Writer.
func Transport() *kafka.Transport {
	return &kafka.Transport{SASL: mechanism()}
}

// Dialer devuelve el dialer autenticado que usa el kafka.Reader.
func Dialer() *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism(),
	}
}
