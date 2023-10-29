package framing

import (
	"compress/gzip"
	"context"
	"fmt"
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
	close(output)

	return
}

func (p *gzipFraming) Frameup(ctx context.Context, input <-chan []byte, output chan<- []byte) (retErr error) {
	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, loglang.ContextKeyPluginType, "framing[gzip]")

	w := gzip.NewWriter(channelWriter{ch: output})
	loglang.PumpToWriter(ctx, stop, input, w)

	// close the gzip writer to flush the remaining bytes
	err := w.Close()
	if err != nil {
		return fmt.Errorf("failed to flush gzip: %w", err)
	}

	// then close the output channel to indicate that framing is complete
	close(output) // all framing plugins must close output to signal completion!

	return
}

type channelWriter struct {
	ch chan<- []byte
}

func (cw channelWriter) Write(unsafeBytes []byte) (n int, err error) {
	// !! We MUST make a copy of the slice given to us by gzip !!
	// because the gzip implementation reuses the underlying buffer to start preparing the next chunk
	// so if we don't make a copy now, the data is likely to mutate before it gets written to outputs
	// and that results in corrupted gzip output.
	//
	// yes there is a performance impact, but correctness is more important.
	safeCopy := make([]byte, len(unsafeBytes))
	copy(safeCopy, unsafeBytes)
	cw.ch <- safeCopy
	return len(safeCopy), nil
}
