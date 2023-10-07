package codec

import (
	"loglang/loglang"
	"testing"
)

func TestKvEncodeSingleString(t *testing.T) {
	evt := loglang.NewEvent()
	evt.SetString("fruit", "apple")
	dat, err := kvEncode(evt)
	if err != nil {
		t.Error(err)
	}
	if string(dat) != `fruit="apple" ` {
		t.Error()
	}
}

func TestKvDecodeSingleString(t *testing.T) {
	evt, err := kvDecode([]byte(`fruit="apple"`))
	if err != nil {
		t.Error(err)
	}
	//v := evt.Get("fruitttt")
	v := evt.Get("fruit")
	if v != "apple" {
		t.Error("expected apple")
	}
}

func TestKvEncodeMultiString(t *testing.T) {
	evt := loglang.NewEvent()
	evt.SetString("size", "large")
	evt.SetString("colour", "red")
	evt.SetString("fruit", "apple")
	evt.SetString("peeled", "false")
	dat, err := kvEncode(evt)
	if err != nil {
		t.Error(err)
	}
	if string(dat) != `colour="red" fruit="apple" peeled="false" size="large" ` {
		t.Error()
	}
}

func TestKvDecodeMultiString(t *testing.T) {
	evt, err := kvDecode([]byte(`fruit="apple" fav="banana"`))
	if err != nil {
		t.Error(err)
	}
	if evt.Get("fruit") != "apple" {
		t.Error("expected apple")
	}
	if evt.Get("fav") != "banana" {
		t.Error("expected banana")
	}
}

func TestKvEncodeSingleNumber(t *testing.T) {
	evt := loglang.NewEvent()
	evt.SetInt("age", 25)
	dat, err := kvEncode(evt)
	if err != nil {
		t.Error(err)
	}
	if string(dat) != `age=25 ` {
		t.Error()
	}
}

func TestKvDecodeSingleNumber(t *testing.T) {
	evt, err := kvDecode([]byte(`age=25`))
	if err != nil {
		t.Error(err)
	}
	v := evt.GetInt("age")
	if v != 25 {
		t.Error("expected 25")
	}
}

func TestKvEncodeMultiNumber(t *testing.T) {
	evt := loglang.NewEvent()
	evt.SetInt("high", 77)
	evt.SetInt("low", 12)
	evt.SetInt("mid", 44)
	dat, err := kvEncode(evt)
	if err != nil {
		t.Error(err)
	}
	// ordering is alphabetical using Go strings.sort()
	if string(dat) != `high=77 low=12 mid=44 ` {
		t.Error()
	}
}

func TestKvDecodeMultiNumber(t *testing.T) {
	evt, err := kvDecode([]byte(`age=25 height=184`))
	if err != nil {
		t.Error(err)
	}
	if evt.GetInt("age") != 25 {
		t.Error("expected 25")
	}
	if evt.GetInt("height") != 184 {
		t.Error("expected 184")
	}
}

func TestKvDecodeExtraWhitespace(t *testing.T) {
	evt, err := kvDecode([]byte(`  age=25  `))
	if err != nil {
		t.Error(err)
	}
	if evt.GetInt("age") != 25 {
		t.Error("expected 25")
	}
}

func TestKvDecodeUnquotedString(t *testing.T) {
	evt, err := kvDecode([]byte(`fruit=apple`))
	if err != nil {
		t.Error(err)
	}
	if evt.Get("fruit") != "apple" {
		t.Error("expected apple")
	}
}

func TestKvDecodeEscaping(t *testing.T) {
	evt, err := kvDecode([]byte(`msg="test\""`))
	if err != nil {
		t.Error(err)
	}
	if evt.Get("msg") != `test"` {
		t.Error(`expected test"`)
	}
}

// what about a string containing quotes or equals?

//func TestKvRoundTrip(t *testing.T) {
//	t.Skip("perfect fidelity is not guaranteed because we tolerate unquoted strings on input, but we always output quoted strings.")
//	origStr := "fruit=apple age=25"
//	origBytes := []byte(origStr)
//	evt, err := kvDecode(origBytes)
//	if err != nil {
//		t.Error(err)
//	}
//	finalBytes, err := kvEncode(evt)
//	if err != nil {
//		t.Error(err)
//	}
//
//	finalStr := string(finalBytes)
//	if origStr != finalStr {
//		t.Error("mismatch")
//	}
//	if evt.Get("fruit") != "apple" {
//		t.Error("expected apple")
//	}
//}
