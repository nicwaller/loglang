package codec

import (
	"encoding/binary"
	"fmt"
	"github.com/nicwaller/loglang"
	"regexp"
)

func Auto() loglang.CodecPlugin {
	return &autoCodec{}
}

type autoCodec struct{}

func (p *autoCodec) Encode(evt loglang.Event) ([]byte, error) {
	var c jsonCodec
	return c.Encode(evt)
}

func (p *autoCodec) Decode(dat []byte) (loglang.Event, error) {
	const magicNumberGzip = 0x1f8b
	const magicNumberChunkedGelf = 0x1e0f

	if dat[0] == '{' && dat[len(dat)-1] == '}' {
		var c jsonCodec
		return c.Decode(dat)
	} else if magicNumberGzip == binary.BigEndian.Uint16(dat) {
		return loglang.Event{}, fmt.Errorf("autoCodec doesn't support gzip; use framing for that")
	} else if magicNumberChunkedGelf == binary.BigEndian.Uint16(dat) {
		// detected magic bytes for chunked GELF
		return loglang.Event{}, fmt.Errorf("autoCodec doesn't support chunked GELF; use framing for that")
	} else if dat[0] == '-' && dat[1] == '-' && dat[2] == '-' {
		var c yamlCodec
		return c.Decode(dat)
	} else if apacheCommonLogPattern.Match(dat) {
		var c ncsaCommonLog
		c.schema = loglang.SchemaFlat
		return c.Decode(dat)
	} else {
		c := Plain("message")
		return c.Decode(dat)
	}
}

var apacheCommonLogPattern = regexp.MustCompile(`^(\S*).*\[(.*)\]\s"(\S*)\s(\S*)\s([^"]*)"\s(\S*)\s(\S*)`)
