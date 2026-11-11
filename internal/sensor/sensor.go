// Package sensor simula los sensores de la estación meteorológica.
package sensor

import (
	"math"
	"math/rand"
)

var Directions = []string{"N", "NO", "O", "SO", "S", "SE", "E", "NE"}

// Reading es la telemetría de un nodo. Se serializa como JSON.
type Reading struct {
	Temperatura     float64 `json:"temperatura"`
	Humedad         int     `json:"humedad"`
	DireccionViento string  `json:"direccion_viento"`
}

const (
	mean   = 55.0 // media pedida por la actividad (°C y %)
	stddev = 15.0 // desviación estándar razonable: ~99.7% cae en [10,100]
)

// normal muestrea N(mean, stddev) y la trunca a [0, max].
func normal(max float64) float64 {
	v := rand.NormFloat64()*stddev + mean
	return math.Min(math.Max(v, 0), max)
}

// Generate produce una lectura aleatoria válida.
func Generate() Reading {
	return Reading{
		Temperatura:     math.Round(normal(110)*100) / 100, // [0,110] con 2 decimales
		Humedad:         int(math.Round(normal(100))),      // [0,100] entero
		DireccionViento: Directions[rand.Intn(len(Directions))],
	}
}
