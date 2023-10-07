package input

import (
	"fmt"
	"log/slog"
	"loglang/loglang"
	"time"
)

func Generator(opts GeneratorOptions) loglang.InputPlugin {
	if opts.ID == "" {
		opts.ID = "Generator"
	}
	if opts.Field == "" {
		opts.Field = "message"
	}
	if opts.Message == "" {
		opts.Message = "Generator"
	}
	if opts.Interval < time.Second {
		opts.Interval = time.Second
	}
	return loglang.InputPlugin{
		Name: opts.ID,
		Run: func(events chan loglang.Event) error {
			slog.Info(fmt.Sprintf("starting generator[%s]", opts.ID))
			for count := 0; ; count++ {
				evt := loglang.NewEvent()
				evt.Field(opts.Field).SetString(opts.Message)
				evt.Field("count").SetInt(count)
				events <- evt
				time.Sleep(opts.Interval)
			}
		},
	}
}

type GeneratorOptions struct {
	ID       string
	Message  string
	Field    string
	Interval time.Duration
	Count    int
}
