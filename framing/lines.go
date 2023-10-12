package framing

import (
	"bufio"
	"bytes"
	"context"
	"github.com/nicwaller/loglang"
	"io"
)

//goland:noinspection GoUnusedExportedFunction
func Lines() loglang.FramingPlugin {
	return &lines{}
}

type lines struct{}

func (p *lines) Extract(ctx context.Context, streams <-chan io.Reader, out chan<- io.Reader) error {
	var streamScanner *bufio.Scanner

	for {
		select {

		case nextStream, hasMoreStreams := <-streams:
			// I'm not totally sure about this code.
			if !hasMoreStreams {
				return nil
			}
			streamScanner = bufio.NewScanner(nextStream)
		scanLoop:
			for {
				select {
				case <-ctx.Done():
					break scanLoop
				default:
					scannerHasMore := streamScanner.Scan()
					if !scannerHasMore {
						break scanLoop
					}
					// PERF: allocating new reader for every line feels wasteful
					out <- bytes.NewReader(streamScanner.Bytes())
				}
			}

		case <-ctx.Done():
			return nil

		}
	}
}

func (p *lines) Frameup(ctx context.Context, packets <-chan io.Reader, out io.Writer) error {
	for {
		select {

		case packet := <-packets:
			// FIXME: we should verify the packet doesn't contain \n otherwise the output stream will be corrupted
			_, err := io.Copy(out, packet)
			if err != nil {
				return err
			}
			_, err = out.Write([]byte{'\n'})
			if err != nil {
				return err
			}

		case <-ctx.Done():
			return nil

		}
	}
}
