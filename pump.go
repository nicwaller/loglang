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
func PumpToWriter(ctx context.Context, stop context.CancelCauseFunc, input <-chan []byte, output io.Writer) {
	log := ContextLogger(ctx)
	log.Debug("started pump to writer")
writeLoop:
	for {
		select {
		case <-ctx.Done():
			break writeLoop
		case chunk, more := <-input:
			//log.Debug("channel pump got a chunk")
			if !more {
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
	log.Debug("stopped pumping input", "cause", context.Cause(ctx))
}

// intended to be run as a goroutine
func PumpFromReader(ctx context.Context, stop context.CancelCauseFunc, input io.Reader, output chan<- []byte) {
	log := ContextLogger(ctx)
	log.Debug("started pump from reader")

	buf := make([]byte, MaxFrameSize)
	count := 0
readLoop:
	for {
		select {
		case <-ctx.Done():
			log.Debug("halting PumpFromReader.readloop", "cause", context.Cause(ctx))
			break readLoop
		default:
			bytesRead, err := input.Read(buf)
			//log.Debug("Reader pump got bytes")
			if err != nil {
				if err.Error() == "EOF" {
					// FIXME: this is a horrible hack to prevent early exit and loss of in-flight frames
					time.Sleep(25 * time.Millisecond)

					// EOF is very normal and expected
					stop(err)
				} else {
					stop(fmt.Errorf("PumpFromReader failed: %w", err))
				}
				break readLoop
			}
			if bytesRead == MaxFrameSize {
				log.Warn("reached MaxFrameSize; cannot guarantee frame integrity")
				//stop(maxErr)
				//break readLoop
			}
			// why copy the frame? to avoid races with slices referencing the buffer
			frameCopy := make([]byte, bytesRead)
			copy(frameCopy, buf)
			output <- frameCopy
			count++
		}
	}
	log.Debug("stopped pumping output",
		"cause", context.Cause(ctx),
		"count", count)
}
