package codec

import (
	"github.com/nicwaller/loglang"
	"log/slog"
)

//goland:noinspection GoUnusedExportedFunction
func Plain(fieldName string) loglang.CodecPlugin {
	return &plainCodec{fieldName: fieldName}
}

type plainCodec struct {
	fieldName string
}

func (p *plainCodec) Encode(event loglang.Event) ([]byte, error) {
	if v, err := event.Field(p.fieldName).Get(); err == nil {
		s := v.(string)
		return []byte(s), nil
	} else {
		slog.Warn("missing field; falling back to kvEncode()")
		// PERF: it's wasteful to always allocate new Kv objects
		return Kv().Encode(event)
	}

}
func (p *plainCodec) Decode(dat []byte) (loglang.Event, error) {
	evt := loglang.NewEvent()
	evt.Field(p.fieldName).Set(string(dat))
	return evt, nil

}
