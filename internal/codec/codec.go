// Package codec codifica una lectura del sensor en 3 bytes (24 bits) y la
// decodifica de vuelta, para redes con payload muy limitado como LoRa o Sigfox.
//
// Distribución de los 24 bits (de más a menos significativo):
//
//	bits 23..10  temperatura (14 bits)
//	bits  9..3   humedad      (7 bits)
//	bits  2..0   dirección    (3 bits)
//
// La temperatura es un float en [0, 110.00] con 2 decimales. Multiplicada por
// 100 queda un entero en [0, 11000], y como 2^14 = 16384 > 11000, cabe en 14
// bits sin perder precisión. La humedad (0-100) cabe en 7 bits (2^7 = 128) y
// las 8 direcciones del viento caben en 3 bits (2^3 = 8).
package codec

import (
	"errors"
	"fmt"
	"math"

	"meteo/internal/sensor"
)

// Size es el tamaño del payload codificado, en bytes.
const Size = 3

// Encode convierte una lectura en 3 bytes. Devuelve error si algún valor
// está fuera de rango o la dirección del viento no es válida.
func Encode(r sensor.Reading) ([]byte, error) {
	if r.Temperatura < 0 || r.Temperatura > 110 {
		return nil, fmt.Errorf("temperatura fuera de rango: %v", r.Temperatura)
	}
	if r.Humedad < 0 || r.Humedad > 100 {
		return nil, fmt.Errorf("humedad fuera de rango: %d", r.Humedad)
	}
	dir := -1
	for i, d := range sensor.Directions {
		if d == r.DireccionViento {
			dir = i
			break
		}
	}
	if dir < 0 {
		return nil, fmt.Errorf("dirección de viento inválida: %q", r.DireccionViento)
	}

	temp := uint32(math.Round(r.Temperatura * 100)) // 0..11000
	v := temp<<10 | uint32(r.Humedad)<<3 | uint32(dir)
	return []byte{byte(v >> 16), byte(v >> 8), byte(v)}, nil
}

// Decode convierte 3 bytes de vuelta en una lectura.
func Decode(b []byte) (sensor.Reading, error) {
	if len(b) != Size {
		return sensor.Reading{}, fmt.Errorf("payload de %d bytes, se esperaban %d", len(b), Size)
	}
	v := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])

	temp := v >> 10
	hum := (v >> 3) & 0x7F
	dir := v & 0x7

	if temp > 11000 || hum > 100 {
		return sensor.Reading{}, errors.New("payload con valores fuera de rango")
	}
	return sensor.Reading{
		Temperatura:     float64(temp) / 100,
		Humedad:         int(hum),
		DireccionViento: sensor.Directions[dir],
	}, nil
}
