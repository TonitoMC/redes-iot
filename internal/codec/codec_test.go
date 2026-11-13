package codec

import (
	"testing"

	"meteo/internal/sensor"
)

func TestRoundTrip(t *testing.T) {
	// Lecturas aleatorias
	for i := 0; i < 100000; i++ {
		in := sensor.Generate()
		b, err := Encode(in)
		if err != nil {
			t.Fatalf("encode %+v: %v", in, err)
		}
		if len(b) != Size {
			t.Fatalf("tamaño %d, se esperaba %d", len(b), Size)
		}
		out, err := Decode(b)
		if err != nil {
			t.Fatalf("decode %x: %v", b, err)
		}
		if out != in {
			t.Fatalf("ida y vuelta: %+v -> %+v", in, out)
		}
	}
}

func TestLimits(t *testing.T) {
	for _, in := range []sensor.Reading{
		{Temperatura: 0, Humedad: 0, DireccionViento: "N"},
		{Temperatura: 110, Humedad: 100, DireccionViento: "NE"},
	} {
		b, err := Encode(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := Decode(b)
		if err != nil || out != in {
			t.Fatalf("límite %+v -> %+v (%v)", in, out, err)
		}
	}
}

func TestInvalid(t *testing.T) {
	for _, in := range []sensor.Reading{
		{Temperatura: 110.01, Humedad: 50, DireccionViento: "N"},
		{Temperatura: -1, Humedad: 50, DireccionViento: "N"},
		{Temperatura: 50, Humedad: 101, DireccionViento: "N"},
		{Temperatura: 50, Humedad: 50, DireccionViento: "X"},
	} {
		if _, err := Encode(in); err == nil {
			t.Errorf("se esperaba error para %+v", in)
		}
	}
	if _, err := Decode([]byte{1, 2}); err == nil {
		t.Error("se esperaba error con payload de 2 bytes")
	}
}
