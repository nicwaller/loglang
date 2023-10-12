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
)

// test with echo -n test | nc -u -w0 localhost 9999
func UdpListener(port int, opts UdpListenerOptions) loglang.InputPlugin {
	if opts.Framing == nil {
		opts.Framing = framing.Whole()
	}
	if opts.Codec == nil {
		opts.Codec = codec.Auto()
	}
	return &udpListener{
		port: port,
		opts: opts,
	}
}

type udpListener struct {
	loglang.BaseInputPlugin
	opts UdpListenerOptions
	port int
}

type UdpListenerOptions struct {
	Framing loglang.FramingPlugin
	Codec   loglang.CodecPlugin
	Schema  loglang.SchemaModel
}

func (p *udpListener) Run(ctx context.Context, sender loglang.Sender) error {
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
		// TODO: we probably need a bigger buffer? maybe 65KB for GELF?
		var buf [4096]byte

		// PERF: should we have multiple goroutines receiving in parallel?
		// UDP is not stream based, so we read each individual datagram
		rlen, addr, err := conn.ReadFromUDP(buf[:])
		if err != nil {
			return err
		} else {
			slog.Debug(fmt.Sprintf("got UDP datagram of %d bytes", rlen))
		}

		templ := p.eventTemplate(addr)
		_, err = sender.SendRaw(ctx, templ, bytes.NewReader(buf[:rlen]))
		if err != nil {
			log.Error("problem in udp listener", "error", err)
		}
	}

	return nil
}

func (p *udpListener) eventTemplate(addr *net.UDPAddr) *loglang.Event {
	evt := loglang.NewEvent()

	switch p.Schema {
	case loglang.SchemaNone:
		// don't enrich with any automatic fields
	case loglang.SchemaLogstashFlat:
		evt.Field("host").SetString(addr.IP.String())
	case loglang.SchemaLogstashECS:
		// NOTE: logstash uses [host] instead of [client] but I think that's weird. -NW
		evt.Field("client", "address").SetString(addr.String())
		evt.Field("client", "ip").SetString(addr.IP.String())
		evt.Field("client", "port").SetInt(addr.Port)
		//evt.Field("client", "bytes").SetInt(frameLen)
		evt.Field("server", "port").SetInt(p.port)
		evt.Field("network", "transport").SetString("udp")
	case loglang.SchemaECS:
		// NOTE: logstash uses [host] instead of [client] but I think that's weird. -NW
		evt.Field("client", "address").SetString(addr.String())
		evt.Field("client", "ip").SetString(addr.IP.String())
		evt.Field("client", "port").SetInt(addr.Port)
		//evt.Field("client", "bytes").SetInt(frameLen)
		evt.Field("server", "port").SetInt(p.port)
		evt.Field("network", "transport").SetString("udp")
	case loglang.SchemaFlat:
		evt.Field("remote_addr").SetString(addr.String())
		evt.Field("client.ip").SetString(addr.IP.String())
		evt.Field("client.port").SetInt(addr.Port)
		evt.Field("server.port").SetInt(p.port)
		evt.Field("transport").SetString("udp")
	}

	return &evt
}
