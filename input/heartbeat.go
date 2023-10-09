package input

import (
	"context"
	"github.com/nicwaller/loglang"
	"log/slog"
	"os"
	"time"
)

func Heartbeat(opts HeartbeatOptions) loglang.InputPlugin {
	if opts.Interval < time.Second {
		opts.Interval = time.Second
	}
	p := generator{opts: opts}
	return &p
}

type generator struct {
	opts HeartbeatOptions
}

type HeartbeatOptions struct {
	ID       string
	Interval time.Duration
	Count    int
	Schema   loglang.SchemaModel
}

func (p *generator) Run(ctx context.Context, events chan loglang.Event) error {
	running := true
	go func() {
		select {
		case <-ctx.Done():
			running = false
		}
	}()

	log := slog.With("pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"))

	schema := p.opts.Schema
	if schema == loglang.SchemaNotDefined {
		if pipelineSchema, ok := ctx.Value("schema").(loglang.SchemaModel); ok {
			schema = pipelineSchema
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "loglang"
	}
	opts := p.opts
	for count := 0; running; count++ {
		evt := loglang.NewEvent()
		switch schema {
		case loglang.SchemaECS:
			evt.Field("host", "name").SetString(hostname)
			evt.Field("event", "module").SetString("loglang")
			evt.Field("event", "dataset").SetString("heartbeat")
			evt.Field("event", "sequence").SetInt(count)
		case loglang.SchemaLogstashFlat:
			evt.Field("host").SetString(hostname)
			evt.Field("clock").SetInt(count)
		case loglang.SchemaLogstashECS:
			evt.Field("host", "name").SetString(hostname)
			evt.Field("event", "sequence").SetInt(count)
		case loglang.SchemaFlat:
			fallthrough
		default:
			evt.Field("module").SetString("loglang")
			evt.Field("dataset").SetString("heartbeat")
			evt.Field("sequence").SetInt(count)
		}
		events <- evt
		time.Sleep(opts.Interval)
	}

	log.Debug("stopped generator")
	return nil
}
