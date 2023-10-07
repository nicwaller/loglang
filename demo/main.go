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
			ID:       "gen",
			Interval: time.Minute,
		}),
		input.UdpListener("udpYeah", 9999, codec.Kv()),
	}
	outputs := []loglang.OutputPlugin{
		output.StdOut(codec.Json()),
		//{
		//	Run: func(event loglang.Event) error {
		//		slog.Debug(event.Field("count").GetString())
		//		return nil
		//	},
		//},
	}

	pipeline.Add(loglang.FilterPlugin{
		Name: "populate [host][ip]",
		Run: func(event loglang.Event, send chan<- loglang.Event) error {
			event.Field("host.ip").Set("127.0.0.1")
			send <- event
			send <- event
			return nil
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
