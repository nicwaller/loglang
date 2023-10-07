package codec

import (
	"log/slog"
	"loglang/loglang"
)

func Plain(fieldname string) loglang.CodecPlugin {
	return loglang.CodecPlugin{
		Encode: func(evt loglang.Event) ([]byte, error) {
			if v, err := evt.Field(fieldname).Get(); err == nil {
				s := v.(string)
				return []byte(s), nil
			} else {
				slog.Warn("missing field; falling back to kvEncode()")
				return kvEncode(evt)
			}
		},
		Decode: func(bytes []byte) (loglang.Event, error) {
			evt := loglang.NewEvent()
			evt.Field(fieldname).Set(string(bytes))
			return evt, nil
		},
	}
}
