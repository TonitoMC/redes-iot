// Package kafkacfg centraliza la conexión y autenticación al broker.
package kafkacfg

import (
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	DefaultBroker = "161.35.11.141:9092"
	username      = "estudiante"
	password      = "TryluRandomPass-2!@!"
)

func mechanism() plain.Mechanism {
	return plain.Mechanism{Username: username, Password: password}
}

// Transport para el Writer (SASL_PLAINTEXT / PLAIN).
func Transport() *kafka.Transport {
	return &kafka.Transport{SASL: mechanism()}
}

// Dialer para el Reader (SASL_PLAINTEXT / PLAIN).
func Dialer() *kafka.Dialer {
	return &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism(),
	}
}
