package codec

import (
	"github.com/nicwaller/loglang"
	"testing"
)

func TestSyslogV0_Encode(t *testing.T) {
	evt := loglang.NewEvent()
	evt.Field("@timestamp").SetString("Oct 15 21:27:56")
	evt.Field("message").SetString("Hello, World!")
	evt.Field("level").SetString("143")
	actual, err := SyslogV0().Encode(evt)
	if err != nil {
		t.Error(err)
	}
	expected := `<8>Oct 15 21:27:56 serendipity.local Hello, World!`
	if string(actual) != expected {
		t.Errorf(`Expected "%s" but got "%s"`, expected, actual)
	}
}
