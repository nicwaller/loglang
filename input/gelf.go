package input

import (
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/framing"
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
	return UdpListener(port, UdpListenerOptions{
		Framing: framing.Whole(),
		Codec:   codec.Json(),
	})
}
