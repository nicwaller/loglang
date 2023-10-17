package framing

import (
	"compress/bzip2"
	"context"
	"github.com/nicwaller/loglang"
	"io"
)

//goland:noinspection GoUnusedExportedFunction
func Bzip() loglang.FramingPlugin {
	return &bzipFraming{}
}

type bzipFraming struct{}

func (p *bzipFraming) Extract(ctx context.Context, input <-chan []byte, output chan<- []byte) (retErr error) {
	var haltExtraction context.CancelCauseFunc
	ctx, haltExtraction = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, loglang.ContextKeyPluginType, "framing[bzip]")

	pipeReader, pipeWriter := io.Pipe()
	subreader := bzip2.NewReader(pipeReader)
	// io.Pipe MUST be paired with a goroutine to avoid blocking
	go loglang.PumpReader(ctx, haltExtraction, subreader, output)
	loglang.PumpChannel(ctx, haltExtraction, input, pipeWriter)

	return
}

func (p *bzipFraming) Frameup(_ context.Context, _ <-chan []byte, _ chan<- []byte) (err error) {
	panic("go standard library doesn't support bzip2 compression")
	// https://pkg.go.dev/compress/bzip2
}
