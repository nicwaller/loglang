package framing

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/nicwaller/loglang"
	"io"
)

//goland:noinspection GoUnusedExportedFunction
func Auto() loglang.FramingPlugin {
	return &autoFraming{}
}

type autoFraming struct{}

func (p *autoFraming) Extract(ctx context.Context, streams <-chan io.Reader, out chan<- io.Reader) error {
	for {
		select {

		case nextStream, hasMoreStreams := <-streams:
			// I'm not totally sure about this code.
			if !hasMoreStreams {
				return nil
			}

			// Peek at the first few bytes so we can auto-detect the framing
			peek := make([]byte, 4)
			_, err := io.ReadFull(nextStream, peek)
			if err != nil {
				return err
			}

			// we don't need to do the decoding;
			// we just need to cut the byte stream into frames
			var mode autoFramingMode
			if peek[0] == '-' && peek[1] == '-' && peek[2] == '-' {
				mode = yamlFramingMode
			} else if peek[0] == '{' {
				// json-lines is a common pattern
				mode = linesFramingMode
			} else if peek[0] == 0x1f && peek[1] == 0x8b {
				mode = gzipFramingMode
			} else if peek[0] == 0x1e && peek[1] == 0x0f {
				// detected magic bytes for chunked GELF
				return fmt.Errorf("auto framing doesn't support chunked GELF")
			} else {
				mode = wholeFramingMode
			}
			// TODO: what are bzip magic bytes?

			// derive a new Reader that includes the bytes we peeked at
			fullStream := io.MultiReader(
				bytes.NewReader(peek),
				nextStream,
			)

			// handle each case
			switch mode {
			case linesFramingMode:
				s := bufio.NewScanner(fullStream)
				for s.Scan() {
					out <- bytes.NewReader(s.Bytes())
				}
			case yamlFramingMode:
				s := bufio.NewScanner(fullStream)
				s.Split(scanYaml)
				for s.Scan() {
					// PERF: no need to read this into memory
					document := s.Bytes()
					out <- bytes.NewReader(document)
				}
			case gzipFramingMode:
				gzipReader, err := gzip.NewReader(fullStream)
				if err != nil {
					// TODO: we should probably just try some other framing
					return err
				}
				out <- gzipReader
			case wholeFramingMode:
				out <- fullStream
			default:
				out <- fullStream
			}

		case <-ctx.Done():
			return nil

		}
	}
}

func (p *autoFraming) Frameup(_ context.Context, _ <-chan io.Reader, _ io.Writer) error {
	panic("auto framing is only for receiving")
}

type autoFramingMode int8

const (
	wholeFramingMode autoFramingMode = iota
	linesFramingMode autoFramingMode = iota
	yamlFramingMode  autoFramingMode = iota
	gzipFramingMode  autoFramingMode = iota
	//bzipFramingMode  autoFramingMode = iota
	//zstdFramingMode  autoFramingMode = iota
)

// for use with bufio.Scanner .Split()
func scanYaml(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	const offset = 3
	if i := bytes.Index(data[offset:], []byte("---")); i >= 0 {
		return i + offset, data[0 : i+offset], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}
