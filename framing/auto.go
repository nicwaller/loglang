package framing

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"compress/zlib"
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

func (p *autoFraming) Extract(ctx context.Context, rawInput <-chan []byte, output chan<- []byte) (exErr error) {
	var stop context.CancelCauseFunc
	ctx, stop = context.WithCancelCause(ctx)
	//log := loglang.ContextLogger(ctx)

	// need to read enough to find index of the first \n in most cases
	const peekSize = 240

	pipeReader, pipeWriter := io.Pipe()
	go loglang.PumpToWriter(ctx, stop, rawInput, pipeWriter)

	// TODO: try to make a decision with every packet of data that arrives?
	input := bufio.NewReaderSize(pipeReader, peekSize)
	// FIXME: how can I peek at a longer prelude without blocking? that makes no sense.
	//peek, err := input.Peek(peekSize)
	peek, err := input.Peek(10)
	if err != nil {
		if err.Error() == "unexpected EOF" {
			// actually this is normal for short stdin segments
		} else {
			return err
		}
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
	} else if ix := bytes.IndexRune(peek, '\n'); ix > 0 && ix < 1000 {
		mode = linesFramingMode
	} else {
		mode = wholeFramingMode
	}
	// TODO: what are bzip magic bytes?

	running := true
	go func() {
		// PERF: do we actually leak a goroutine here?
		<-ctx.Done()
		running = false
	}()

	// handle each case
	switch mode {
	case linesFramingMode:
		s := bufio.NewScanner(input)
		for running && s.Scan() {
			output <- s.Bytes()
		}
	case yamlFramingMode:
		s := bufio.NewScanner(input)
		s.Split(scanYaml)
		for running && s.Scan() {
			output <- s.Bytes()
		}
	case gzipFramingMode:
		var subreader io.Reader
		subreader, exErr = gzip.NewReader(input)
		if exErr != nil {
			return
		}
		loglang.PumpFromReader(ctx, stop, subreader, output)
		return
	case bzipFramingMode:
		var subreader io.Reader
		subreader = bzip2.NewReader(input)
		loglang.PumpFromReader(ctx, stop, subreader, output)
		return
	case zstdFramingMode:
		var subreader io.Reader
		subreader, exErr = zlib.NewReader(input)
		if exErr != nil {
			return
		}
		loglang.PumpFromReader(ctx, stop, subreader, output)
		return
	case wholeFramingMode:
		loglang.PumpFromReader(ctx, stop, input, output)
		return
	default:
		loglang.PumpFromReader(ctx, stop, input, output)
		return
	}

	return nil
}

func (p *autoFraming) Frameup(_ context.Context, _ <-chan []byte, _ chan<- []byte) error {
	panic("auto framing is only for receiving")
}

type autoFramingMode string

const (
	wholeFramingMode autoFramingMode = "whole"
	linesFramingMode autoFramingMode = "lines"
	yamlFramingMode  autoFramingMode = "yaml"
	gzipFramingMode  autoFramingMode = "gzip"
	bzipFramingMode  autoFramingMode = "bzip"
	zstdFramingMode  autoFramingMode = "zstd"
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
