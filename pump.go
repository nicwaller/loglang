package loglang

import (
	"context"
	"fmt"
	"io"
	"time"
)

// this function does nothing useful
// anywhere it's used, that is surely a design error
func Pump(ctx context.Context, stop context.CancelCauseFunc, input chan *Event, output chan *Event) {
	// TODO: I think we need this, but also it causes a panic?
	//defer close(output)
nullPump:
	for {
		select {
		case inEvt, more := <-input:
			if !more {
				stop(fmt.Errorf("nullPump saw closed channel"))
				break nullPump
			}
			if inEvt == nil {
				stop(fmt.Errorf("nullPump saw nil event"))
				break nullPump
			}
			output <- inEvt
		case <-ctx.Done():
			break nullPump
		}
	}
}

// intended to be run as a goroutine
func PumpFilterList(ctx context.Context, stop context.CancelCauseFunc,
	input chan *Event, output chan *Event,
	filters []NamedEntity[FilterPlugin], ack bool) {
	// don't need to defer close(output) here because that happens in Pump()

	log := ContextLogger(ctx)
	log.Debug("preparing filter chain")

	// make channels to go between filter stages
	allChannels := make([]chan *Event, 0)
	allChannels = append(allChannels, input)
	for i := 0; i < len(filters); i++ {
		allChannels = append(allChannels, make(chan *Event, 2)) // TODO: why 2?
	}

	// set up goroutines to pump each stage of the Pipeline
	for i, filter := range filters {
		go PumpFilter(ctx, stop, allChannels[i], allChannels[i+1], filter, ack)
	}

	log.Info(fmt.Sprintf("set up filter chain length=%d", len(filters)))

	// FIXME: don't use Pump(); avoid wasting a channel instead.
	Pump(ctx, stop, allChannels[len(allChannels)-1], output)
}

func PumpFilter(ctx context.Context, stop context.CancelCauseFunc,
	input chan *Event, output chan *Event,
	filter NamedEntity[FilterPlugin], ack bool) {
	log := ContextLogger(ctx)
	filterFunc := filter.Value
	log.Debug("starting filter pump")
	defer close(output)
filterPump:
	for {
		select {
		case event, more := <-input:
			if !more {
				stop(fmt.Errorf("filterPump saw closed channel"))
				break filterPump
			}
			if event == nil {
				stop(fmt.Errorf("filterPump saw nil event"))
				break filterPump
			}
			dropped := false
			dropFunc := func() {
				if dropped {
					log.Warn("drop() should only be called once")
				} else {
					dropped = true
				}
			}
			err := filterFunc(event, output, dropFunc)
			// E2E handling
			// FIXME: is this is the right place to check ack?
			if ack && event.batch != nil {
				if err != nil {
					event.batch.errorHappened <- err
				} else if dropped {
					event.batch.dropHappened <- true
				} else {
					event.batch.filterBurndown <- 1
				}
			}
			// regular handling
			if err != nil {
				log.Warn("filter error",
					"error", err,
					"filter", filter.Name,
				)
				// TODO: should events continue down the pipeline if a single filter stage fails?
				// continue
			} else if dropped {
				// do not pass to next stage of filter pipeline
				continue
			}
			// send it
			output <- event
		case <-ctx.Done():
			break filterPump
		}
	}
}

// intended to be run as a goroutine
func PumpToFunction(ctx context.Context, stop context.CancelCauseFunc, input <-chan *Event, fn func(*Event) error) {
	log := ContextLogger(ctx)
	log.Debug("starting pump to function")
functionPump:
	for {
		select {
		case event, more := <-input:
			if !more {
				stop(fmt.Errorf("pumpToFunction saw closed channel"))
				break functionPump
			}
			if event == nil {
				stop(fmt.Errorf("functionPump saw nil event"))
				break functionPump
			}
			err := fn(event)
			if err != nil {
				log.Warn(fmt.Sprintf("functionPump saw error: %w", err))
				// I don't think we want to stop the pipeline over this.
				//stop(fmt.Errorf("functionPump error: %w", err))
				//break functionPump
			}
		case <-ctx.Done():
			break functionPump
		}
	}
}

// intended to be run as a goroutine
func PumpFanOut(ctx context.Context, stop context.CancelCauseFunc, input <-chan *Event, outputs []chan *Event) {
	defer func() {
		for i := range outputs {
			close(outputs[i])
		}
	}()
	log := ContextLogger(ctx)
	log.Debug("starting pump fanOut")
fanOut:
	for {
		// TODO: maybe include a timeout case?
		select {
		case event, more := <-input:
			if !more {
				stop(fmt.Errorf("fanOut saw closed channel"))
				break fanOut
			}
			if event == nil {
				stop(fmt.Errorf("fanOut saw nil event"))
				break fanOut
			}
			for i := range outputs {
				outputs[i] <- event
			}
		case <-ctx.Done():
			break fanOut
		}
	}
}

// intended to be run as a goroutine
func PumpToWriter(ctx context.Context, stop context.CancelCauseFunc, input <-chan []byte, output io.WriteCloser) {
	log := ContextLogger(ctx)
	log.Debug("started pumping data to writer")
	defer output.Close()
writeLoop:
	for {
		select {
		case <-time.After(15 * time.Second):
			log.Debug("stalled?")
		case <-ctx.Done():
			break writeLoop
		case chunk, more := <-input:
			if !more {
				log.Debug("channel pump saw closed stream")
				stop(fmt.Errorf("input channel closed"))
				break writeLoop
			}
			_, err := output.Write(chunk)
			if err != nil {
				stop(err)
				break writeLoop
			}
		}
	}
	log.Debug("stopped pumping data to writer", "cause", context.Cause(ctx))
}

// intended to be run as a goroutine
func PumpFromReader(ctx context.Context, stop context.CancelCauseFunc, input io.Reader, output chan<- []byte) {
	defer close(output)
	log := ContextLogger(ctx)
	log.Debug("started pump from reader")

	buf := make([]byte, MaxFrameSize)
	count := 0

	chunks := make(chan []byte)
	go func() {
		// this is a hacky workaround for input.Read() being blocking
		// and not being able to pick up an EOF
		// and this function never returning
		for {
			// input.Read() blocks forever
			bytesRead, err := input.Read(buf)
			if err != nil {
				if err == io.EOF {
					// EOF is very normal and expected
					stop(err)
				} else {
					stop(fmt.Errorf("PumpFromReader failed: %w", err))
				}
				return
			}
			if bytesRead == MaxFrameSize {
				log.Warn("reached MaxFrameSize; cannot guarantee frame integrity")
			}
			// why copy the frame? to avoid races with slices referencing the buffer
			frameCopy := make([]byte, bytesRead)
			copy(frameCopy, buf)
			chunks <- frameCopy
		}
		// this probably leaks a goroutine.
		// can we recover without shutting down the whole process?
	}()
readLoop:
	for {
		select {
		case <-ctx.Done():
			log.Debug("halting PumpFromReader.readloop", "cause", context.Cause(ctx))
			break readLoop
		case <-time.After(100 * time.Millisecond):
			// todo much longer timeout
			log.Debug("reader timed out")
			break readLoop
		case chunk, more := <-chunks:
			if !more {
				stop(fmt.Errorf("end of chunks"))
				break readLoop
			}
			output <- chunk
			count++
		}
	}
	log.Debug("stopped pumping output",
		"cause", context.Cause(ctx),
		"count", count)
}
