package loglang

import (
	"context"
	"fmt"
	"io"
	"log/slog"
)

type InputPlugin interface {
	Run(context.Context, Sender) error
	Extract(context.Context, *Event, io.Reader, chan *Event) error
	//Extractor
}

type Extractor func(context.Context, *Event, io.Reader, chan *Event) error

type BaseInputPlugin struct {
	Framing []FramingPlugin
	Codec   CodecPlugin
	Schema  SchemaModel
}

func (p *BaseInputPlugin) Run(_ context.Context, _ Sender) error {
	// Run should be overridden by plugin implementations
	return fmt.Errorf("not implemented")
}

// Extract runs all the framing stages and codec. Output is a channel of decoded Events.
func (p *BaseInputPlugin) Extract(ctx context.Context, template *Event, reader io.Reader, output chan *Event) error {
	slog.Debug("              BaseInputPlugin.Extract()")
	if p.Codec == nil {
		panic("input codec must not be nil")
	}

	log := slog.Default().With(
		"pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"),
	)

	switch len(p.Framing) {
	case 0:
		// TODO: should we fall back to something else? Lines()
		panic("no framing configured on input plugin")

	case 1:
		stage := p.Framing[0]
		// The happy path is exactly one level of framing
		// because we exactly one io.Reader to think about
		justOneReader := make(chan io.Reader, 1)
		justOneReader <- reader
		frames := make(chan io.Reader)

		// start producing frames
		go func() {
			slog.Debug("              BaseInputPlugin.Extract().goroutine")
			err := stage.Extract(ctx, justOneReader, frames)
			slog.Debug("                BaseInputPlugin.Extract().goroutine closed frames channel")
			close(frames)
			if err != nil {
				log.Error("framing stage failed", "error", err)
			}
			slog.Debug("              BaseInputPlugin.Extract().goroutine completed")
		}()
		// consume those frames
		contextCancelled := ctx.Done()
		slog.Debug("                BaseInputPlugin.Extract().frameLoop started")
	frameLoop:
		for {
			select {
			case frame, b := <-frames:
				if !b {
					slog.Debug("                  BaseInputPlugin.Extract() frames channel closed")
					break frameLoop
				}
				dat, err := io.ReadAll(frame)
				if err != nil {
					return fmt.Errorf("de-framing failed: %w", err)
				}
				slog.Debug("                  BaseInputPlugin.Extract() got a frame " + fmt.Sprintf("%d bytes", len(dat)))
				evt, err := p.Codec.Decode(dat)
				evt.Merge(template, false)
				if err != nil {
					return fmt.Errorf("frame decoding failed: %w", err)
				}
				output <- &evt
			case <-contextCancelled:
				fmt.Println("context cancelled")
				break frameLoop
			}
		}
		slog.Debug("                BaseInputPlugin.Extract().frameLoop finished")
		slog.Debug("              BaseInputPlugin.Extract() returned")
		return nil

	case 2, 3, 4:
		// we should be prepared for multiple levels of framing
		// select{} doesn't work well with arrays of channels, so the max is hardcoded
		// TODO: write this hard code for handling variable number of stages, all in the same goroutine
		// what about running Extract() recursively?
		// I don't think we want to spin up multiple goroutines for *each and every event*!
		return fmt.Errorf("not yet implemented: multiple framing stages")

	default:
		panic("too many framing stages")

	}

	return nil
}

type OutputPlugin interface {
	// TODO: Run() needs a better Name or purpose
	Send(context.Context, []*Event) error
}

type FilterPlugin func(event *Event, inject chan<- *Event, drop func()) error

type CodecPlugin interface {
	Encode(Event) ([]byte, error)
	Decode([]byte) (Event, error)
	// FIXME: change to *Event
}

type FramingPlugin interface {
	// return type must be io.Reader
	// usually framing returns single events (small)
	// but it might also be decompression of a 5GB object in S3
	Extract(context.Context, <-chan io.Reader, chan<- io.Reader) error
	Frameup(context.Context, <-chan io.Reader, io.Writer) error
	// the lack of symmetry here feels very weird to me -NW
}
