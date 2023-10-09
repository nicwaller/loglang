package input

import (
	"context"
	"github.com/nicwaller/loglang"
	"log/slog"
	"time"
)

func Generator(opts GeneratorOptions) loglang.InputPlugin {
	if opts.Field == "" {
		opts.Field = "message"
	}
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
	Message  string
	Field    string
	Interval time.Duration
	Count    int
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

	opts := p.opts
	for count := 0; running; count++ {
		evt := loglang.NewEvent()
		evt.Field("event", "module").SetString("loglang")
		evt.Field("event", "dataset").SetString("heartbeat")
		evt.Field("event", "sequence").SetInt(count)
		if opts.Field != "" && opts.Message != "" {
			evt.Field(opts.Field).SetString(opts.Message)
		}
		events <- evt
		time.Sleep(opts.Interval)
	}

	log.Debug("stopped generator")
	return nil
}
