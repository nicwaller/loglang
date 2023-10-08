package input

import (
	"fmt"
	"log/slog"
	"loglang/loglang"
	"loglang/loglang/framing"
	"net"
	"strconv"
	"time"
)

func TcpListener(name string, eventType string, port int, codec loglang.CodecPlugin) loglang.InputPlugin {
	return loglang.InputPlugin{
		Name: name,
		Type: eventType,
		Run: func(send chan loglang.Event) error {
			slog.Debug(fmt.Sprintf("TCP listener starting on %s:%d", name, port),
				"server.port", port, "log.logger", name,
			)
			ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
			if err != nil {
				return err
			}
			for {
				conn, err := ln.Accept()
				if err != nil {
					// handle error
				}
				go tcpReading(conn, send, framing.Lines(), codec)
			}
		},
	}
}

// TODO: practice with "lines" framing
func tcpReading(conn net.Conn, send chan loglang.Event, framer loglang.FramingPlugin, codec loglang.CodecPlugin) {
	frames := make(chan []byte)
	go func() {
		slog.Debug("starting framer")
		err := framer.Run(conn, frames)
		if err != nil {
			slog.Error(err.Error())
			return
		}
	}()

	go func() {
		for {
			select {
			case frame := <-frames:
				slog.Debug("got a frame of ? bytes")
				evt, err := codec.Decode(frame)
				if err != nil {
					slog.Error(err.Error())
				} else {
					send <- evt
				}
			case <-time.After(30 * time.Second):
				return
			}

		}
	}()
}
