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
	loglang.BaseInputPlugin
	opts HeartbeatOptions
}

type HeartbeatOptions struct {
	ID       string
	Interval time.Duration
	Count    int
	Schema   loglang.SchemaModel
}

func (p *generator) Run(ctx context.Context, sender loglang.Sender) error {
	running := true
	go func() {
		<-ctx.Done()
		running = false
	}()

	log := slog.With("pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"))

	sender.SetE2E(true)

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
	var lastDuration time.Duration
	for count := 0; running; count++ {
		nextHeartbeat := time.After(opts.Interval)
		evt := loglang.NewEvent()
		switch schema {
		case loglang.SchemaNone:
			// don't enrich with any automatic fields
		case loglang.SchemaECS:
			evt.Field("message").SetString("heartbeat")
			evt.Field("host", "name").SetString(hostname)
			evt.Field("event", "module").SetString("loglang")
			evt.Field("event", "dataset").SetString("heartbeat")
			evt.Field("event", "sequence").SetInt(count)
			if lastDuration > 0 {
				evt.Field("event", "duration").SetInt(int(lastDuration))
			}
		case loglang.SchemaLogstashFlat:
			evt.Field("host").SetString(hostname)
			evt.Field("clock").SetInt(count)
		case loglang.SchemaLogstashECS:
			evt.Field("host", "name").SetString(hostname)
			evt.Field("event", "sequence").SetInt(count)
		case loglang.SchemaFlat:
			fallthrough
		default:
			evt.Field("message").SetString("heartbeat")
			evt.Field("module").SetString("loglang")
			evt.Field("dataset").SetString("heartbeat")
			evt.Field("sequence").SetInt(count)
		}
		log.Debug("sending heartbeat")
		// heartbeats can be slowed down by the filter pipeline
		// it's possible to send heartbeats faster by wrapping this in a goroutine
		// but that just masks the fact that heartbeats SHOULD be fast
		// don't worry about lost heartbeats
		// the pipeline will generate errors if batches time out
		result := sender.Send(&evt)
		if result != nil {
			if !result.Ok {
				log.
					With("error", result.Summary()).
					Error("heartbeat failed")
			}
			lastDuration = result.Finish.Sub(result.Start)
		}
		<-nextHeartbeat
	}

	log.Debug("stopped generator")
	return nil
}
