package framing

import (
	"compress/zlib"
	"context"
	"github.com/nicwaller/loglang"
	"io"
)

//goland:noinspection GoUnusedExportedFunction
func Zstd() loglang.FramingPlugin {
	return &zstdFraming{}
}

type zstdFraming struct{}

func (p *zstdFraming) Extract(ctx context.Context, input <-chan []byte, output chan<- []byte) (retErr error) {
	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, loglang.ContextKeyPluginType, "framing[zlib]")

	pipeReader, pipeWriter := io.Pipe()
	subreader, err := zlib.NewReader(pipeReader)
	if err != nil {
		return err
	}

	// io.Pipe MUST be paired with a goroutine to avoid blocking
	go loglang.PumpChannel(ctx, stop, input, pipeWriter)
	loglang.PumpReader(ctx, stop, subreader, output)

	return
}

func (p *zstdFraming) Frameup(ctx context.Context, input <-chan []byte, output chan<- []byte) (retErr error) {
	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, loglang.ContextKeyPluginType, "framing[zlib]")

	readCompressed, pipeWriter := io.Pipe()
	writePlain, retErr := zlib.NewWriterLevel(pipeWriter, zlib.BestSpeed)
	if retErr != nil {
		return
	}

	// io.Pipe MUST be paired with a goroutine to avoid blocking
	go loglang.PumpChannel(ctx, stop, input, writePlain)
	loglang.PumpReader(ctx, stop, readCompressed, output)

	return
}
