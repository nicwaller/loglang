package input

import (
	"context"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/framing"
	"log/slog"
	"net"
	"strconv"
)

func NewTcpListener(port int, opts TcpListenerOptions) loglang.InputPlugin {
	// FIXME: this needs to be more global?
	t := &tcpListener{
		port: port,
		opts: opts,
	}
	t.Framing = []loglang.FramingPlugin{framing.Lines()}
	t.Codec = codec.Auto()
	return t
}

type tcpListener struct {
	loglang.BaseInputPlugin
	port int
	opts TcpListenerOptions
}

type TcpListenerOptions struct{}

func (p *tcpListener) Run(ctx context.Context, sender loglang.Sender) error {
	log := slog.Default().With(
		"pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"),
		"server.port", strconv.Itoa(p.port),
	)

	running := true
	go func() {
		<-ctx.Done()
		running = false
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
		// TODO: prepare a better template event, like UDP listener
		// TODO: is there a Context for TCP connection?
		result, err := sender.SendRaw(ctx, nil, conn)
		_, _ = conn.Write([]byte(result.Summary()))
	}
	log.Debug("stopped")
	return nil
}
