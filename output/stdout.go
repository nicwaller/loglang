package output

import (
	"context"
	"github.com/nicwaller/loglang"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
)

func StdOut(opts StdoutOptions) loglang.OutputPlugin {
	return &stdOut{opts: opts}
}

type stdOut struct {
	opts StdoutOptions
}

type StdoutOptions struct {
}

func (p *stdOut) Send(ctx context.Context, events []*loglang.Event, cp loglang.CodecPlugin, fp loglang.FramingPlugin) error {
	ctx = context.WithValue(ctx, loglang.ContextKeyPluginType, "stdout")
	log := loglang.ContextLogger(ctx)

	encodedEvents := make(chan []byte)
	framedEvents := make(chan []byte)

	var wg sync.WaitGroup
	wg.Add(3)

	// encode each event and dump it into a framing channel
	go func() {
		for _, evt := range events {
			dat, err := cp.Encode(*evt)
			if err != nil {
				slog.Error("error", "error", err)
				close(encodedEvents)
			}
			encodedEvents <- dat
		}
		close(encodedEvents)
		wg.Done()
	}()

	// run the specified framing
	go func() {
		fp.Frameup(ctx, encodedEvents, framedEvents)
		wg.Done()
	}()

	// write the framing results to stdout
	go func() {
	outLoop:
		for {
			select {
			case frame, more := <-framedEvents:
				if !more {
					log.Debug("exiting loop", "more", more, "remain", len(frame))
					break outLoop
				}
				write, err := os.Stdout.Write(frame)
				if err != nil {
					log.Error(err.Error())
				} else {
					log.Info(strconv.Itoa(write))
				}
			case <-time.After(3 * time.Second):
				log.Warn("stdout stalled; make sure that framing plugin is closing channel")
			}
		}
		wg.Done()
	}()

	wg.Wait()

	return nil
}
