package codec

import (
	"github.com/nicwaller/loglang"
	"testing"
)

func TestPlainCodec_Encode(t *testing.T) {
	evt := loglang.NewEvent()
	evt.Field("message").SetString("Hello, World!")
	dat, err := Plain("message").Encode(evt)
	if err != nil {
		t.Error(err)
	}
	if string(dat) != "Hello, World!" {
		t.Errorf(`Expected "Hello, World!" but got "%s"`, dat)
	}
}

func TestPlainCodec_Decode(t *testing.T) {
	evt, err := Plain("message").Decode([]byte("Hello, World!"))
	if err != nil {
		t.Error(err)
	}
	if v := evt.Field("message").GetString(); v != "Hello, World!" {
		t.Errorf(`Expected "Hello, World!" but got "%s"`, v)
	}
}
