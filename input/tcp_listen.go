package input

import (
	"github.com/nicwaller/loglang"
	"log/slog"
	"net"
	"strconv"
	"time"
)

func NewTcpListener(port int, opts TcpListenerOptions) loglang.InputPlugin {
	return &tcpListener{
		port: port,
		opts: opts,
	}
}

type tcpListener struct {
	port int
	opts TcpListenerOptions
}

type TcpListenerOptions struct {
	Framing loglang.FramingPlugin
	Codec   loglang.CodecPlugin
}

func (t *tcpListener) Run(send chan loglang.Event) error {
	log := slog.Default().With(
		"server.port", strconv.Itoa(t.port),
	)
	log.Debug("listening")

	ln, err := net.Listen("tcp", ":"+strconv.Itoa(t.port))
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Warn("failed accepting tcp connection")
		}
		go tcpReading(conn, send, t.opts.Framing, t.opts.Codec)
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
