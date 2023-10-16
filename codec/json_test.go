package codec

import (
	"github.com/nicwaller/loglang"
	"testing"
)

func TestJsonCodec_Encode(t *testing.T) {
	evt := loglang.NewEvent()
	evt.Field("message").SetString("Hello, World!")
	actual, err := Json().Encode(evt)
	if err != nil {
		t.Error(err)
	}
	expected := `{"message":"Hello, World!"}`
	if string(actual) != expected {
		t.Errorf(`Expected "%s" but got "%s"`, expected, actual)
	}
}

func TestJsonCodec_Decode(t *testing.T) {
	evt, err := Json().Decode([]byte(`{"message": "Hello, World!"}`))
	if err != nil {
		t.Error(err)
	}

	// TODO: test full equivalency of the event
	expected := `Hello, World!`
	if actual := evt.Field("message").GetString(); actual != expected {
		t.Errorf(`Expected "%s" but got "%s"`, expected, actual)
	}
}
