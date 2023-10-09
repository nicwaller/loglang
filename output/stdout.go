package output

import (
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

func (p *stdOut) Run(event loglang.Event) error {
	dat, err := p.opts.Codec.Encode(event)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(dat)
	_, err = os.Stdout.Write([]byte("\n"))
	if err != nil {
		return err
	} else {
		return nil
	}
}
