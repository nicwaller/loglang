package codec

import (
	"github.com/nicwaller/loglang"
	"testing"
)

func TestKvEncodeSingleString(t *testing.T) {
	evt := loglang.NewEvent()
	evt.Field("fruit").SetString("apple")
	dat, err := Kv().Encode(evt)
	if err != nil {
		t.Error(err)
	}
	if string(dat) != `fruit="apple" ` {
		t.Error()
	}
}

func TestKvDecodeSingleString(t *testing.T) {
	evt, err := Kv().Decode([]byte(`fruit="apple"`))
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
	evt.Field("size").SetString("large")
	evt.Field("colour").SetString("red")
	evt.Field("fruit").SetString("apple")
	evt.Field("peeled").SetString("false")
	dat, err := Kv().Encode(evt)
	if err != nil {
		t.Error(err)
	}
	if string(dat) != `colour="red" fruit="apple" peeled="false" size="large" ` {
		t.Error()
	}
}

func TestKvDecodeMultiString(t *testing.T) {
	evt, err := Kv().Decode([]byte(`fruit="apple" fav="banana"`))
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
	evt.Field("age").SetInt(25)
	dat, err := Kv().Encode(evt)
	if err != nil {
		t.Error(err)
	}
	if string(dat) != `age=25 ` {
		t.Error()
	}
}

func TestKvDecodeSingleNumber(t *testing.T) {
	evt, err := Kv().Decode([]byte(`age=25`))
	if err != nil {
		t.Error(err)
	}
	v := evt.Field("age").GetInt()
	if v != 25 {
		t.Error("expected 25")
	}
}

func TestKvEncodeMultiNumber(t *testing.T) {
	evt := loglang.NewEvent()
	evt.Field("high").SetInt(77)
	evt.Field("low").SetInt(12)
	evt.Field("mid").SetInt(44)
	dat, err := Kv().Encode(evt)
	if err != nil {
		t.Error(err)
	}
	// ordering is alphabetical using Go strings.sort()
	if string(dat) != `high=77 low=12 mid=44 ` {
		t.Error()
	}
}

func TestKvDecodeMultiNumber(t *testing.T) {
	evt, err := Kv().Decode([]byte(`age=25 height=184`))
	if err != nil {
		t.Error(err)
	}
	if evt.Field("age").MustGet() != 25 {
		t.Error("expected 25")
	}
	if evt.Field("height").MustGet() != 184 {
		t.Error("expected 184")
	}
}

func TestKvDecodeExtraWhitespace(t *testing.T) {
	evt, err := Kv().Decode([]byte(`  age=25  `))
	if err != nil {
		t.Error(err)
	}
	if evt.Field("age").MustGet() != 25 {
		t.Error("expected 25")
	}
}

func TestKvDecodeUnquotedString(t *testing.T) {
	evt, err := Kv().Decode([]byte(`fruit=apple`))
	if err != nil {
		t.Error(err)
	}
	if evt.Get("fruit") != "apple" {
		t.Error("expected apple")
	}
}

func TestKvDecodeEscaping(t *testing.T) {
	evt, err := Kv().Decode([]byte(`msg="test\""`))
	if err != nil {
		t.Error(err)
	}
	if evt.Get("msg") != `test"` {
		t.Error(`expected test"`)
	}
}

func TestKvDecodeBareString(t *testing.T) {
	evt, err := Kv().Decode([]byte(`hello`))
	if err != nil {
		t.Error(err)
	}
	if evt.Get("hello") != true {
		t.Error(`expected true"`)
	}
}

// what about a string containing quotes or equals?

//func TestKvRoundTrip(t *testing.T) {
//	t.Skip("perfect fidelity is not guaranteed because we tolerate unquoted strings on input, but we always output quoted strings.")
//	origStr := "fruit=apple age=25"
//	origBytes := []byte(origStr)
//	evt, err := Kv().Decode(origBytes)
//	if err != nil {
//		t.Error(err)
//	}
//	finalBytes, err := Kv().Encode(evt)
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
