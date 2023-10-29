package loglang

import (
	"context"
	"fmt"
	"io"
	"time"
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
	if p.Codec == nil {
		panic("input codec must not be nil")
	}

	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, ContextKeyPluginType, "BaseInputPlugin")
	log := ContextLogger(ctx)

	switch len(p.Framing) {
	case 0:
		// TODO: should we fall back to something else? Lines()
		panic("no framing configured on input plugin")

	case 1:
		// collect chunks from the reader
		chunks := make(chan []byte)
		go PumpFromReader(ctx, stop, reader, chunks)

		// the framing stage will normalize those into whole frames
		stage := p.Framing[0]
		frames := make(chan []byte)
		go func() {
			err := stage.Extract(ctx, chunks, frames)
			close(frames)
			if err != nil {
				log.Error("framing stage failed", "error", err)
				stop(err)
			}
		}()

		// run the decoder on those frames here in this thread
		decoded := 0
	decoderLoop:
		for {
			// should there be a timeout on this selecct?
			select {
			case <-ctx.Done():
				log.Debug("decoderLoop finished by context",
					"cause", context.Cause(ctx),
					"count", decoded)
				break decoderLoop
			case frame, more := <-frames:
				if !more {
					log.Debug("decoderLoop finished",
						"cause", "frames channel closed",
						"count", decoded)
					break decoderLoop
				}
				evt, err := p.Codec.Decode(frame)
				evt.Merge(template, false)
				if err != nil {
					return fmt.Errorf("frame decoding failed: %w", err)
				}
				output <- &evt
				decoded++
			case <-time.After(time.Second * 2):
				log.Debug("decoderLoop timeout", "count", decoded)
			}
		}
		// this close() is important!
		close(output)

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
	// Output interface uses function instead of channel
	// because the function return indicates when an event has been processed
	// and that drives end-to-end acknowledgement back to the input
	Send(context.Context, []*Event, CodecPlugin, FramingPlugin) error
}

type FilterPlugin func(event *Event, inject chan<- *Event, drop func()) error

type CodecPlugin interface {
	Encode(Event) ([]byte, error)
	Decode([]byte) (Event, error)
	// An Event struct is only 16 bytes in memory
	// so pointers aren't needed in this interface
}

type FramingPlugin interface {
	// I tried using io.Reader and io.Writer but because they use fixed buffers,
	// they had problems if the frame size exceeded the buffer size.
	Extract(ctx context.Context, input <-chan []byte, output chan<- []byte) error
	Frameup(ctx context.Context, input <-chan []byte, output chan<- []byte) error
}

const MaxFrameSize = 65536
