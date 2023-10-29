package framing

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/nicwaller/loglang"
	"io"
	"log/slog"
)

//goland:noinspection GoUnusedExportedFunction
func Lines() loglang.FramingPlugin {
	return &lines{}
}

type lines struct{}

func (p *lines) Extract(ctx context.Context, input <-chan []byte, output chan<- []byte) (retErr error) {
	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	log := loglang.ContextLogger(ctx)

	scannable, writeInputFrames := io.Pipe()
	go loglang.PumpToWriter(ctx, stop, input, writeInputFrames)

	streamScanner := bufio.NewScanner(scannable)
	log.Info("started scanLoop")
	running := true
scanLoop:
	for running {
		select {
		case <-ctx.Done():
			break scanLoop
		default:
			running = streamScanner.Scan()
			output <- streamScanner.Bytes()
		}
	}
	log.Info("stopped scanLoop")
	return
}

func (p *lines) Frameup(ctx context.Context, input <-chan []byte, output chan<- []byte) error {
framingLoop:
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		case frame, more := <-input:
			if !more {
				slog.Debug("framingLoop saw closed channel")
				break framingLoop
			}
			const linefeed = byte('\n')
			if bytes.IndexByte(frame, linefeed) > 0 {
				return fmt.Errorf("cannot safely encode frames that contain a linefeed")
			}
			frameLF := append(frame, linefeed)
			output <- frameLF
		}
	}
	close(output)
	return nil
}
