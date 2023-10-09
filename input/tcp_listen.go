package input

import (
	"context"
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

func (p *tcpListener) Run(ctx context.Context, send chan loglang.Event) error {
	log := slog.Default().With(
		"pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"),
		"server.port", strconv.Itoa(p.port),
	)

	running := true
	go func() {
		select {
		case <-ctx.Done():
			running = false
		}
	}()

	ln, err := net.Listen("tcp", ":"+strconv.Itoa(p.port))
	log.Debug("listening")
	if err != nil {
		return err
	}

	for running {
		conn, err := ln.Accept()
		if err != nil {
			log.Warn("failed accepting tcp connection")
		}
		go tcpReading(ctx, conn, send, p.opts.Framing, p.opts.Codec)
	}
	log.Debug("stopped")
	return nil
}

// TODO: practice with "lines" framing
func tcpReading(ctx context.Context, conn net.Conn, send chan loglang.Event, framer loglang.FramingPlugin, codec loglang.CodecPlugin) {
	frames := make(chan []byte)
	go func() {
		slog.Debug("starting framer")
		err := framer.Run(ctx, conn, frames)
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
