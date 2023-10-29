package framing

import (
	"compress/gzip"
	"context"
	"github.com/nicwaller/loglang"
	"io"
)

//goland:noinspection GoUnusedExportedFunction
func Gzip() loglang.FramingPlugin {
	return &gzipFraming{}
}

type gzipFraming struct{}

func (p *gzipFraming) Extract(ctx context.Context, input <-chan []byte, output chan<- []byte) (retErr error) {
	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, loglang.ContextKeyPluginType, "framing[gzip]")
	log := loglang.ContextLogger(ctx)

	pipeReader, pipeWriter := io.Pipe()
	subreader, err := gzip.NewReader(pipeReader)
	if err != nil {
		log.Error("failed to create gzip reader")
		return err
	}

	// io.Pipe MUST be paired with a goroutine to avoid blocking
	go loglang.PumpToWriter(ctx, stop, input, pipeWriter)
	loglang.PumpFromReader(ctx, stop, subreader, output)

	return
}

func (p *gzipFraming) Frameup(ctx context.Context, input <-chan []byte, output chan<- []byte) (retErr error) {
	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, loglang.ContextKeyPluginType, "framing[gzip]")

	readCompressed, pipeWriter := io.Pipe()
	writePlain, retErr := gzip.NewWriterLevel(pipeWriter, gzip.BestSpeed)
	if retErr != nil {
		return
	}

	// io.Pipe MUST be paired with a goroutine to avoid blocking
	go loglang.PumpToWriter(ctx, stop, input, writePlain)
	loglang.PumpFromReader(ctx, stop, readCompressed, output)

	return
}
