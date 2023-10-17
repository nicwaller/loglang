package loglang

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// PERF: this is a memory-intensive part of the program.
// a single S3 object can easily be 5 GB.
// we don't want to hold all that in memory, so we need to use io.Reader and stream processing.
// we usually cannot just collect all the events into a slice.

type Sender interface {
	// send one or more fully-formed events
	Send(...*Event) *BatchResult
	// send a template + byte stream; let the pipeline choose framing & codec
	SendRaw(context.Context, *Event, io.Reader) (*BatchResult, error)
	// send byte stream, but prescribe a specific framing and codec strategy
	SendWithFramingCodec(context.Context, *Event, FramingPlugin, CodecPlugin, io.Reader) (*BatchResult, error)
	// Opt-in for end-to-end acknowledgement
	SetE2E(bool)
}

type SimpleSender struct {
	e2e     bool
	events  chan *Event
	extract Extractor
	ctx     context.Context
	fanout  int
}

func NewSender(ctx context.Context, events chan *Event, extract Extractor, fanout int) *SimpleSender {
	return &SimpleSender{
		e2e:     false,
		events:  events,
		extract: extract,
		ctx:     ctx,
		fanout:  fanout,
	}
}

func (s *SimpleSender) Send(events ...*Event) *BatchResult {
	if s.e2e {
		b := newBatch(len(events), s.fanout)
		for _, event := range events {
			event.batch = b
			s.events <- event
		}
		// TODO: maybe don't ignore this error? log it?
		result, _ := b.waitForResults()
		return result
	} else {
		for _, event := range events {
			s.events <- event
		}
		return nil
	}
}

func (s *SimpleSender) SendRaw(ctx context.Context, template *Event, byteStream io.Reader) (*BatchResult, error) {
	log := ContextLogger(ctx)

	// FIXME: make better decisions about what framing/codec to use
	// like, it should be a variable on the struct
	//return s.SendWithFramingCodec(template, framing.Lines(), codec.Auto(), byteStream)
	//	type Extractor func(context.Context, *Event, io.Reader, chan *Event) error

	// err := s.extract(s.ctx, template, byteStream, events)

	if s.e2e {
		// prepare the batch
		b := newBatch(-1, s.fanout)
		count := 0

		// get started for real
		events := make(chan *Event)
		go func() {
			// TODO: use request context or sender context?
			//err := s.extract(s.ctx, template, byteStream, events)
			err := s.extract(ctx, template, byteStream, events)
			if err != nil {
				log.Error("error", "error", err)
			}
		}()

		go func() {
			for evt := range events {
				evt.Merge(template, false)
				evt.batch = b
				s.events <- evt
				count++
			}
			b.batchSize = count
		}()

		// FIXME: count is not known ahead of time so batch exits early when it should not
		result, err := b.waitForResults()
		close(s.events)
		return result, err
	} else {
		// get started for real
		events := make(chan *Event)
		go func() {
			// TODO: use request context or sender context?
			//err := s.extract(s.ctx, template, byteStream, events)
			err := s.extract(ctx, template, byteStream, events)
			if err != nil {
				slog.Error("error", "error", err)
			}
			close(events)
		}()

		for evt := range events {
			if evt == nil {
				log.Error("SendRaw saw nil event")
				continue
			}
			evt.Merge(template, false)
			select {
			case s.events <- evt:
				// this space left intentionally blank
				// this is an unconditional non-blocking write
				// but... why?
			case <-time.After(100 * time.Millisecond):
				log.Warn("timeout")
			}
		}
		close(s.events)

		return nil, nil
	}
}

func (s *SimpleSender) SendWithFramingCodec(ctx context.Context, template *Event, f FramingPlugin, c CodecPlugin, byteStream io.Reader) (*BatchResult, error) {
	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, ContextKeyPluginType, "SimpleSender")
	log := ContextLogger(ctx)

	if s.e2e {
		// prepare the batch
		b := newBatch(-1, s.fanout)
		count := 0

		// collect chunks from the reader
		chunks := make(chan []byte)
		go PumpReader(ctx, stop, byteStream, chunks)

		// the framing stage will normalize those into whole frames
		stage := f
		frames := make(chan []byte)

		go func() {
			err := stage.Extract(ctx, chunks, frames)
			if err != nil {
				log.Error("framing stage failed", "error", err)
				stop(err)
			}
		}()

		go func() {
			for frameData := range frames {
				evt, err := c.Decode(frameData)
				if err != nil {
					//failed <- err
				}
				evt.Merge(template, false)
				evt.batch = b
				s.events <- &evt
				count++
			}
			b.batchSize = count
		}()

		return b.waitForResults()
	} else {
		// collect chunks from the reader
		chunks := make(chan []byte)
		go PumpReader(ctx, stop, byteStream, chunks)

		// the framing stage will normalize those into whole frames
		stage := f
		frames := make(chan []byte)
		go func() {
			err := stage.Extract(ctx, chunks, frames)
			if err != nil {
				log.Error("framing stage failed", "error", err)
				stop(err)
			}
		}()

		//go func() {
		for frameData := range frames {
			evt, err := c.Decode(frameData)
			if err != nil {
				//failed <- err
			}
			evt.Merge(template, false)
			s.events <- &evt
		}
		//}()

		return nil, nil
	}
}

func (s *SimpleSender) SetE2E(e2e bool) {
	s.e2e = e2e
}

type publishingBatch struct {
	filterBurndown chan int
	outputBurndown chan int
	dropHappened   chan bool
	errorHappened  chan error
	batchSize      int
	outputFanout   int
	opts           *PipelineOptions
}

func newBatch(batchSize int, outputFanout int) *publishingBatch {
	return &publishingBatch{
		filterBurndown: make(chan int),
		outputBurndown: make(chan int),
		dropHappened:   make(chan bool),
		errorHappened:  make(chan error),
		batchSize:      batchSize,
		outputFanout:   outputFanout,
		opts:           nil,
	}
}

func (b *publishingBatch) waitForResults() (*BatchResult, error) {
	pendingFilter := b.batchSize
	pendingOutput := b.batchSize * b.outputFanout

	slowBatchWarning := make(<-chan time.Time)
	if b.opts != nil && b.opts.SlowBatchWarning > 0 {
		slowBatchWarning = time.After(b.opts.SlowBatchWarning)
	}

	slowBatchDeadline := make(<-chan time.Time)
	if b.opts != nil && b.opts.BatchTimeout > 0 {
		slowBatchDeadline = time.After(b.opts.SlowBatchWarning)
	}

	result := &BatchResult{
		TotalCount:   b.batchSize,
		DropCount:    0,
		ErrorCount:   0,
		SuccessCount: 0,
		Ok:           true,
		Errors:       make([]error, 0),
		Start:        time.Now(), // TODO: can we set this earlier?
	}

	for pendingFilter > 0 || pendingOutput > 0 {
		select {
		case x := <-b.filterBurndown:
			// x should normally be 1
			pendingFilter -= x
		case x := <-b.outputBurndown:
			// x should normally be 1
			pendingOutput -= x
			// TODO: how do I increment success count? how do I know a given event was published to ALL filters?
		case <-b.dropHappened:
			pendingFilter -= 1
			// if dropped by a filter, it will never reach any of the outputs
			pendingOutput -= b.outputFanout
			result.DropCount++
		case err := <-b.errorHappened:
			// TODO: should we set the Ok status or not?
			//result.Ok = false
			result.ErrorCount += 1
			result.Errors = append(result.Errors, err)
		case <-slowBatchWarning:
			slog.Warn("publishingBatch is taking longer than expected")
		case <-slowBatchDeadline:
			result.Ok = false
			return result, fmt.Errorf("publishingBatch timed out")
		}
	}

	result.Finish = time.Now()
	return result, nil
}

type BatchResult struct {
	TotalCount   int
	DropCount    int
	ErrorCount   int
	SuccessCount int
	Ok           bool
	Errors       []error
	Start        time.Time
	Finish       time.Time
}

func (r *BatchResult) Summary() string {
	return fmt.Sprintf("Ok=%t TotalCount=%d SuccessCount=%d DropCount=%d ErrorCount=%d",
		r.Ok, r.TotalCount, r.SuccessCount, r.DropCount, r.ErrorCount)
}
