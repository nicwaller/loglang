package input

import (
	"context"
	"github.com/nicwaller/loglang"
	"log/slog"
	"time"
)

func Generator(opts GeneratorOptions) loglang.InputPlugin {
	if opts.Interval < time.Second {
		opts.Interval = time.Second
	}
	p := generator{opts: opts}
	return &p
}

type generator struct {
	opts GeneratorOptions
}

type GeneratorOptions struct {
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

	opts := p.opts
	for count := 0; running; count++ {
		evt := loglang.NewEvent()
		if schema == loglang.SchemaElasticCommonSchema {
			evt.Field("event", "module").SetString("loglang")
			evt.Field("event", "dataset").SetString("heartbeat")
			evt.Field("event", "sequence").SetInt(count)
		} else {
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
