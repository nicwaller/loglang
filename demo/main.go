package main

import (
	"github.com/lmittmann/tint"
	"log/slog"
	"loglang/loglang"
	"loglang/loglang/codec"
	"loglang/loglang/input"
	"loglang/loglang/output"
	"os"
	"time"
)

func main() {
	setupLogging()

	pipeline := loglang.NewPipeline()

	inputs := []loglang.InputPlugin{
		input.Generator(input.GeneratorOptions{
			ID: "gen",
		}),
		input.UdpListener("udpYeah", 9999, codec.Kv()),
	}
	outputs := []loglang.OutputPlugin{
		output.StdOut(codec.Kv()),
		{
			Run: func(event loglang.Event) error {
				slog.Debug(event.Field("count").GetString())
				return nil
			},
		},
	}

	pipeline.Add(loglang.FilterPlugin{
		Run: func(event loglang.Event) (loglang.Event, error) {
			event.Field("host.ip").Set("127.0.0.1")
			event.Field("time").Set(time.Now())
			return event, nil
		},
	})

	//pipeline.Add(filter.Json("test1", "message"))
	//pipeline.Add(filter.Rename("?", "msg", "message"))

	_ = pipeline.Run(inputs, outputs)
}

func setupLogging() {
	w := os.Stderr

	// create a new logger
	//logger := slog.New(tint.NewHandler(w, nil))

	// set global logger with custom options
	slog.SetDefault(slog.New(
		tint.NewHandler(w, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.Kitchen,
		}),
	))
}
