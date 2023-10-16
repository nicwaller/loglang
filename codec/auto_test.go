package codec

import (
	"testing"
)

func TestAutoCodec_DecodeJson(t *testing.T) {
	evt, err := Auto().Decode([]byte(`{"message": "Hello, World!"}`))
	if err != nil {
		t.Error(err)
	}

	// TODO: test full equivalency of the event
	expected := `Hello, World!`
	if actual := evt.Field("message").GetString(); actual != expected {
		t.Errorf(`Expected "%s" but got "%s"`, expected, actual)
	}

}

func TestAutoCodec_DecodeYaml(t *testing.T) {
	evt, err := Auto().Decode([]byte(`---
message: Hello, World!`))
	if err != nil {
		t.Error(err)
	}

	// TODO: test full equivalency of the event
	expected := `Hello, World!`
	if actual := evt.Field("message").GetString(); actual != expected {
		t.Errorf(`Expected "%s" but got "%s"`, expected, actual)
	}
}
