package output

import (
	"context"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"os"
)

func StdOut(opts StdoutOptions) loglang.OutputPlugin {
	if opts.Codec == nil {
		opts.Codec = codec.Kv()
	}
	return &stdOut{opts: opts}
}

type stdOut struct {
	opts StdoutOptions
}

type StdoutOptions struct {
	Codec loglang.CodecPlugin
}

func (p *stdOut) Send(_ context.Context, events []*loglang.Event) error {
	for _, event := range events {
		if err := p.sendOne(event); err != nil {
			return err
		}
	}
	return nil
}

func (p *stdOut) sendOne(event *loglang.Event) error {
	dat, err := p.opts.Codec.Encode(*event)
	if err != nil {
		return err
	}
	// PERF: might want to use buffered output to go faster
	// but be careful about flushing the buffer before exit
	_, err = os.Stdout.Write(dat)
	_, err = os.Stdout.Write([]byte("\n"))
	if err != nil {
		return err
	} else {
		return nil
	}
}
