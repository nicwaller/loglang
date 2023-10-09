package codec

import (
	"bytes"
	"github.com/nicwaller/loglang"
	"log/slog"
	"strconv"
	"strings"
)

// simple key/value pairs on a single line
// example:
//
//	key1=value key2=value
//
// See also:
//   - Logstash calls this "kv"
//     https://www.elastic.co/guide/en/logstash/current/plugins-filters-kv.html
//   - Fluentd/Fluentbit calls this "logfmt"
//     https://docs.fluentbit.io/manual/pipeline/parsers/logfmt
func Kv() loglang.CodecPlugin {
	return loglang.CodecPlugin{
		Name:   "",
		Encode: kvEncode,
		Decode: kvDecode,
	}
}

// TODO: have an option for flat or deep encoding decoding
// TODO: support encoding/decoding nested maps
// TODO: probably should be using this library: https://pkg.go.dev/github.com/kr/logfmt?utm_source=godoc
// TODO: support other delimiters

func kvEncode(evt loglang.Event) ([]byte, error) {
	var sb strings.Builder
	evt.TraverseFields(func(field loglang.Field) {
		// FIXME: ignoring errors, oooh dangerous
		value, _ := field.Get()
		sb.WriteString(strings.Join(field.Path, "."))
		sb.WriteString(`=`)
		if strVal, ok := value.(string); ok {
			strVal = strings.ReplaceAll(strVal, `"`, `\"`)
			sb.WriteString(`"` + strVal + `"`)
		} else if intVal, ok := value.(int); ok {
			sb.WriteString(strconv.Itoa(intVal))
		} else if intVal, ok := value.(int64); ok {
			sb.WriteString(strconv.Itoa(int(intVal)))
		} else if floatVal, ok := value.(float64); ok {
			sb.WriteString(strconv.FormatFloat(floatVal, 'f', -1, 64))
		} else if boolVal, ok := value.(bool); ok {
			sb.WriteString(strconv.FormatBool(boolVal))
		} else {
			panic("unhandled type")
		}
		// TODO: trim the trailing space
		sb.WriteString(` `)

	})
	return []byte(sb.String()), nil
}

func kvDecode(dat []byte) (loglang.Event, error) {
	evt := loglang.NewEvent()
	// FIXME: this parser is very bad
	for _, field := range bytes.Split(dat, []byte{' '}) {
		if string(field) == "" {
			// is there a better way to .split ignoring repeating delimiters in Go?
			continue
		}
		keyB, valueB, didCut := bytes.Cut(field, []byte{'='})
		if !didCut {
			if len(keyB) == 0 {
				slog.Warn("kv decoder encountered weird empty string")
				continue
			} else {
				evt.Field(string(field)).SetBool(true)
				continue
			}
		}

		key := string(keyB)
		value := string(valueB)
		quot := `"`
		if strings.HasSuffix(value, quot) && strings.HasPrefix(value, quot) {
			// FIXME: This may trim too much. eg `foo="bar\""
			//value = strings.Trim(value, `"`)
			value = value[1 : len(value)-1]
			value = strings.ReplaceAll(value, `\"`, `"`)
			evt.Field(key).SetString(value)
		} else {
			if intVal, err := strconv.Atoi(value); err == nil {
				evt.Field(key).SetInt(intVal)
			} else {
				// FIXME: support float here?
				evt.Field(key).SetString(value)
			}
		}
		// FIXME: handle bool, null
	}
	return evt, nil
}
