package codec

import (
	"github.com/nicwaller/loglang"
	"gopkg.in/yaml.v3"
)

func Yaml() loglang.CodecPlugin {
	return &yamlCodec{}
}

type yamlCodec struct{}

func (p *yamlCodec) Encode(event loglang.Event) ([]byte, error) {
	return yaml.Marshal(event.Fields)
}

func (p *yamlCodec) Decode(dat []byte) (loglang.Event, error) {
	evt := loglang.NewEvent()
	err := yaml.Unmarshal(dat, &evt.Fields)
	return evt, err
}
