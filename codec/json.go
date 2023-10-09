package codec

import (
	"encoding/json"
	"github.com/nicwaller/loglang"
)

func Json() loglang.CodecPlugin {
	return &jsonCodec{}
}

type jsonCodec struct{}

func (p *jsonCodec) Encode(event loglang.Event) ([]byte, error) {
	// TODO: maybe use a different JSON encoder with deterministic ordering?
	return json.Marshal(event.Fields)
}

func (p *jsonCodec) Decode(dat []byte) (loglang.Event, error) {
	evt := loglang.NewEvent()
	err := json.Unmarshal(dat, &evt.Fields)
	return evt, err
}
