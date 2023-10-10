package codec

import (
	"github.com/nicwaller/loglang"
)

func Auto() loglang.CodecPlugin {
	return &jsonCodec{}
}

type autoCodec struct{}

func (p *autoCodec) Encode(_ loglang.Event) ([]byte, error) {
	panic("codec[auto] does not support encoding")
}

func (p *autoCodec) Decode(dat []byte) (loglang.Event, error) {
	if dat[0] == '{' && dat[len(dat)-1] == '}' {
		var c jsonCodec
		return c.Decode(dat)
	} else if dat[0] == 0x1f && dat[1] == 0x8b {
		panic("codec[auto] does not support GZIP")
	} else if dat[0] == 0x1e && dat[1] == 0x0f {
		panic("codec[auto] does not support Chunked GELF")
	} else if dat[0] == '-' && dat[1] == '-' && dat[2] == '-' {
		panic("codec[auto] does not support YAML")
	} else {
		var c kvCodec
		return c.Decode(dat)
	}
}
