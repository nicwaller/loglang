package input

import (
	"github.com/nicwaller/loglang"
	"time"
)

func Generator(opts GeneratorOptions) loglang.InputPlugin {
	if opts.Field == "" {
		opts.Field = "message"
	}
	if opts.Interval < time.Second {
		opts.Interval = time.Second
	}
	p := generator{opts: opts}
	return &p
}

type generator struct {
	opts GeneratorOptions
}

type GeneratorOptions struct {
	ID       string
	Message  string
	Field    string
	Interval time.Duration
	Count    int
}

func (p *generator) Run(events chan loglang.Event) error {
	opts := p.opts
	for count := 0; ; count++ {
		evt := loglang.NewEvent()
		evt.Field("count").SetInt(count)
		if opts.Field != "" && opts.Message != "" {
			evt.Field(opts.Field).SetString(opts.Message)
		}
		events <- evt
		time.Sleep(opts.Interval)
	}
}
