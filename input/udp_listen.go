package input

import (
	"bytes"
	"context"
	"fmt"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/framing"
	"log/slog"
	"net"
	"strconv"
	"time"
)

// test with echo -n test | nc -u -w0 localhost 9999
func UdpListener(port int, opts UdpListenerOptions) loglang.InputPlugin {
	if opts.Framing == nil {
		opts.Framing = framing.Whole()
	}
	if opts.Codec == nil {
		opts.Codec = codec.Kv()
	}
	return &udpListener{
		port: port,
		opts: opts,
	}
}

type udpListener struct {
	opts UdpListenerOptions
	port int
}

type UdpListenerOptions struct {
	Framing loglang.FramingPlugin
	Codec   loglang.CodecPlugin
	Schema  loglang.SchemaModel
}

func (p *udpListener) Run(ctx context.Context, events chan loglang.Event) error {
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

	schema := p.opts.Schema
	if schema == loglang.SchemaNotDefined {
		if pipelineSchema, ok := ctx.Value("schema").(loglang.SchemaModel); ok {
			schema = pipelineSchema
		}
	}

	addr := net.UDPAddr{
		Port: p.port,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenUDP("udp", &addr) // code does not block here
	log.Debug("listening")
	if err != nil {
		log.Error(err.Error())
		return err
	}
	defer func(conn *net.UDPConn) {
		_ = conn.Close()
	}(conn)

	for running {
		var buf [4096]byte
		// UDP is not stream based, so we read each individual datagram
		rlen, addr, err := conn.ReadFromUDP(buf[:])
		if rlen == 1 {
			// FIXME : wtf is this X
			continue
		}
		if err != nil {
			return err
		}
		slog.Debug(fmt.Sprintf("got UDP datagram of %d bytes", rlen))

		frames := make(chan []byte)
		go func() {
			err := p.opts.Framing.Run(ctx, bytes.NewReader(buf[:rlen]), frames)
			if err != nil {
				slog.Error(err.Error())
			}
		}()
		go func() {
			for {
				select {
				case frame := <-frames:
					//slog.Debug(fmt.Sprintf("got a frame of %d bytes", len(frame)))
					evt, err := p.opts.Codec.Decode(frame)
					if schema == loglang.SchemaECS {
						// NOTE: logstash uses [host] instead of [client] but I think that's weird. -NW
						evt.Field("client", "address").SetString(addr.String())
						evt.Field("client", "ip").SetString(addr.IP.String())
						evt.Field("client", "port").SetInt(addr.Port)
						evt.Field("client", "bytes").SetInt(len(frame))
						evt.Field("server", "port").SetInt(p.port)
						evt.Field("network", "transport").SetString("udp")
					} else if schema == loglang.SchemaLogstashFlat {
						evt.Field("host").SetString(addr.IP.String())
					} else {
						evt.Field("remote_addr").SetString(addr.String())
						evt.Field("client.ip").SetString(addr.IP.String())
						evt.Field("client.port").SetInt(addr.Port)
						evt.Field("server.port").SetInt(p.port)
						evt.Field("transport").SetString("udp")
					}
					if err != nil {
						slog.Error(fmt.Errorf("lost whole datagram or part of datagram: %w", err).Error())
					} else {
						events <- evt
					}
				case <-time.After(30 * time.Second):
					return
				}

			}
		}()
	}

	return nil
}
