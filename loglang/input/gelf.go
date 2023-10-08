package input

import (
	"log/slog"
	"loglang/loglang"
	"loglang/loglang/codec"
	"loglang/loglang/filter"
	"loglang/loglang/framing"
)

// GELF: Graylog Extended Log Format
// Example:
//
//	{
//	 "version": "1.1",
//	 "host": "example.org",
//	 "short_message": "A short message that helps you identify what is going on",
//	 "full_message": "Backtrace here\n\nmore stuff",
//	 "timestamp": 1385053862.3072,
//	 "level": 1,
//	 "_user_id": 9001,
//	 "_some_info": "foo",
//	 "_some_env_var": "bar"
//	}
func GelfUDP(port int) loglang.InputPlugin {
	p := UdpListener("GELF-UDP", "gelf", port, framing.Whole(), codec.Json())
	p.Filters = append(p.Filters, loglang.FilterPlugin{
		Run: func(event loglang.Event, events chan<- loglang.Event) error {
			if event.Field("version").GetString() != "1.1" {
				slog.Warn("received GELF message with unsupported version")
			}
			if event.Field("level").GetString() == "" {
				slog.Warn("received GELF message with missing level")
			}
			events <- event
			return nil
		},
	})
	p.Filters = append(p.Filters, filter.Rename("", "level", "log.level"))
	p.Filters = append(p.Filters, filter.Rename("", "short_message", "message"))
	return p
}
