// Package sensor simula los sensores de la estación meteorológica.
//
// En una estación real la temperatura, la humedad y la dirección del viento
// las entregan componentes electrónicos. Aquí se reemplazan por generadores
// de números pseudoaleatorios, tal como pide la actividad:
//
//   - Temperatura: float en [0, 110] °C con 2 decimales.
//   - Humedad relativa: entero en [0, 100] %.
//   - Dirección del viento: una de 8 direcciones (N, NO, O, SO, S, SE, E, NE).
//
// La temperatura y la humedad se muestrean de una distribución normal con
// media 55, para que los valores queden "centrados" en el rango. Los valores
// que se salen del rango se recortan al límite más cercano.
//
// Una lectura completa se representa con Reading y se envía como JSON, por
// ejemplo: {"temperatura":56.32,"humedad":51,"direccion_viento":"SO"}
package sensor

import (
	"math"
	"math/rand"
)

// Directions son las direcciones posibles del viento.
var Directions = []string{"N", "NO", "O", "SO", "S", "SE", "E", "NE"}

// Reading es una lectura completa de los tres sensores.
// Los tags json definen los nombres de los campos en el mensaje enviado.
type Reading struct {
	Temperatura     float64 `json:"temperatura"`      // °C, 2 decimales
	Humedad         int     `json:"humedad"`          // %, entero
	DireccionViento string  `json:"direccion_viento"` // Ver Directions
}

const (
	mean   = 55.0 // Media de la distribución (°C y %)
	stddev = 15.0 // Desviación estándar: deja los valores lejos de los bordes del rango
)

// Normal devuelve una muestra de una distribución normal (mean, stddev)
// recortada al intervalo [0, max].
func normal(max float64) float64 {
	v := rand.NormFloat64()*stddev + mean
	return math.Min(math.Max(v, 0), max)
}

// Generate simula una lectura de los tres sensores.
func Generate() Reading {
	return Reading{
		Temperatura:     math.Round(normal(110)*100) / 100,
		Humedad:         int(math.Round(normal(100))),
		DireccionViento: Directions[rand.Intn(len(Directions))],
	}
}
