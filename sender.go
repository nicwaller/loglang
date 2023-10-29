package loglang

import (
	"context"
	"fmt"
	"io"
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
		b := newBatch()
		for _, event := range events {
			event.batch = b
			s.events <- event
		}
		counted := make(chan int, 1)
		counted <- len(events)
		// TODO: maybe don't ignore this error? log it?
		result, _ := b.waitForResults(context.TODO(), counted)
		return result
	} else {
		for _, event := range events {
			s.events <- event
		}
		return nil
	}
}

// treat the entire contents of io.Reader as a single batch
// this can be useful, for example, when using S3+SQS
// because the SQS event should not be acknowledged until the entire batch
// of events from the S3 object has been fully written to all outputs
//
// function is named SendRaw because it's sending raw byte stream reader
// deferring the framing and codec decisions to the pipeline configuration
func (s *SimpleSender) SendRaw(ctx context.Context, template *Event, byteStream io.Reader) (*BatchResult, error) {
	log := ContextLogger(ctx)

	// FIXME: make better decisions about what framing/codec to use
	// like, it should be a variable on the struct
	//return s.SendWithFramingCodec(template, framing.Lines(), codec.Auto(), byteStream)
	//	type Extractor func(context.Context, *Event, io.Reader, chan *Event) error

	// err := s.extract(s.ctx, template, byteStream, events)

	if s.e2e {
		// prepare the batch
		b := newBatch()
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

		counted := make(chan int, 1)

		// this needs to run in a goroutine to keep the channels open
		// otherwise we'll deadlock (stuck channels)
		go func() {
			for evt := range events {
				evt.Merge(template, false)
				evt.batch = b
				s.events <- evt
				count++
			}
			counted <- count
		}()

		result, err := b.waitForResults(ctx, counted)
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
				log.Error("error", "error", err)
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
		b := newBatch()
		count := 0

		// collect chunks from the reader
		chunks := make(chan []byte)
		go PumpFromReader(ctx, stop, byteStream, chunks)

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
		}()

		cc := make(chan int, 1)
		cc <- count
		return b.waitForResults(ctx, cc)
	} else {
		// collect chunks from the reader
		chunks := make(chan []byte)
		go PumpFromReader(ctx, stop, byteStream, chunks)

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
	outputFanout   int
	slowWarning    time.Duration
	slowDeadline   time.Duration
}

func newBatch() *publishingBatch {
	return &publishingBatch{
		filterBurndown: make(chan int),
		outputBurndown: make(chan int),
		dropHappened:   make(chan bool),
		errorHappened:  make(chan error),
		// TODO: make these customizable
		slowWarning:  3 * time.Second,
		slowDeadline: 10 * time.Second,
	}
}

// we don't always know how many events we're waiting for
// because we start monitoring the batch progress before the entire batch is read in
// that's why we need a channel to get the final count
func (b *publishingBatch) waitForResults(ctx context.Context, counted chan int) (*BatchResult, error) {
	log := ContextLogger(ctx)

	countFilterMarked := 0
	countOutputMarked := 0

	slowWarning := time.After(b.slowWarning)
	slowDeadline := time.After(b.slowDeadline)

	result := &BatchResult{
		//TotalCount:   b.batchSize,
		DropCount:    0,
		ErrorCount:   0,
		SuccessCount: 0,
		Ok:           true,
		Errors:       make([]error, 0),
		Start:        time.Now(), // TODO: can we set this earlier?
	}

	// FIXME: when to stop for loop?
	target := -1
	for target != countOutputMarked {
		select {
		case target = <-counted:
			// good, now we know the termination condition
		case x := <-b.filterBurndown:
			if x != 1 {
				panic(x)
			}
			countFilterMarked += 1
		case x := <-b.outputBurndown:
			if x != 1 {
				panic(x)
			}
			countOutputMarked += 1
			result.SuccessCount += 1
		case <-b.dropHappened:
			countFilterMarked += 1
			countOutputMarked += 1
			result.DropCount++
		case err := <-b.errorHappened:
			// TODO: should we set the Ok status or not?
			//result.Ok = false
			result.ErrorCount += 1
			result.Errors = append(result.Errors, err)
		case <-slowWarning:
			log.Warn(fmt.Sprintf("input batch still pending after %v (target=%d)", b.slowWarning, target))
		case <-slowDeadline:
			result.Ok = false
			errTxt := fmt.Sprintf("input batch failed after timeout: %v (target=%d)", b.slowDeadline, target)
			log.Error(errTxt)
			return result, fmt.Errorf(errTxt)
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
