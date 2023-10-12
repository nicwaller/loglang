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

type gzipFraming struct {
	// TODO: make this configurable somehow
	FailuresAreFatal bool
}

func (p *gzipFraming) Extract(ctx context.Context, streams <-chan io.Reader, out chan<- io.Reader) error {
	for {
		select {

		case nextStream := <-streams:
			gzipReader, err := gzip.NewReader(nextStream)
			if p.FailuresAreFatal && err != nil {
				return err
			}
			out <- gzipReader

		case <-ctx.Done():
			return nil

		}
	}
}

// do NOT call this for each event, for performance reasons
// it NEEDS to handle multiple events in a channel
func (p *gzipFraming) Frameup(ctx context.Context, packets <-chan io.Reader, out io.Writer) error {
	gzipWriter := gzip.NewWriter(out)
	for {
		select {

		case packet := <-packets:
			_, err := io.Copy(gzipWriter, packet)
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return nil

		}
	}
}
