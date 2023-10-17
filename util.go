package loglang

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"
)

func Map[I any, O any](mapper func(I) O, orig []I) []O {
	changed := make([]O, 0, len(orig))
	for _, v := range orig {
		changed = append(changed, mapper(v))
	}
	return changed
}

func CoalesceStr(args ...string) string {
	for _, v := range args {
		if v != "" {
			return v
		}
	}
	return ""
}

func Coalesce(args ...any) any {
	for _, v := range args {
		if v != nil {
			switch v.(type) {
			case string:
				if v.(string) != "" {
					return v
				}
			default:
				return v
			}
		}
	}
	return nil
}

type NamedEntity[T any] struct {
	Value T
	Name  string
}

func loggerFromContext(ctx context.Context) *slog.Logger {
	return slog.Default().
		With("pipeline", ctx.Value("pipeline")).
		With("plugin", ctx.Value("plugin"))
}

// intended to be run as a goroutine
func PumpChannel(ctx context.Context, stop context.CancelCauseFunc, input <-chan []byte, output io.Writer) {
	log := ContextLogger(ctx)
	log.Debug("started pumping input")
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
func PumpReader(ctx context.Context, stop context.CancelCauseFunc, input io.Reader, output chan<- []byte) {
	log := ContextLogger(ctx)
	log.Debug("started pumping output")

	buf := make([]byte, MaxFrameSize)
	count := 0
readLoop:
	for {
		select {
		case <-ctx.Done():
			log.Debug("halting PumpReader.readloop", "cause", context.Cause(ctx))
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
					stop(fmt.Errorf("PumpReader failed: %w", err))
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
