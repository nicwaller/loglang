package codec

import (
	"encoding/json"
	"loglang/loglang"
)

func Json() loglang.CodecPlugin {
	return loglang.CodecPlugin{
		Name:   "",
		Encode: jsonEncode,
		Decode: jsonDecode,
	}
}

func jsonEncode(evt loglang.Event) ([]byte, error) {
	// TODO: maybe use a different JSON encoder with deterministic ordering?
	return json.Marshal(evt.Fields)
}

func jsonDecode(dat []byte) (loglang.Event, error) {
	evt := loglang.NewEvent()
	//fields := make(map[string]any)
	err := json.Unmarshal(dat, &evt.Fields)
	//for k, v := range fields {
	//	if strVal, ok := v.(string); ok {
	//		evt.SetString(k, strVal)
	//	} else if numVal, ok := v.(float64); ok {
	//		if numVal == float64(int64(numVal)) {
	//			evt.SetInt(k, int(numVal))
	//		} else {
	//			evt.SetFloat(k, numVal)
	//		}
	//	} else {
	//		slog.Error(fmt.Sprintf("lost field %s of unknown type", k))
	//	}
	//	// FIXME: handle bool type
	//}
	return evt, err
}

// TODO: dedupe the JSON handling code (codec uses filter or vice versa?)
