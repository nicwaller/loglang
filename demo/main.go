package main

import (
	"github.com/lmittmann/tint"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/input"
	"github.com/nicwaller/loglang/output"
	"log/slog"
	"os"
	"time"
)

func main() {
	setupLogging()

	p := loglang.NewPipeline("demo", loglang.PipelineOptions{})

	//p.Input("heartbeat", input.Generator(input.GeneratorOptions{Interval: time.Second}))
	p.Input("tcp/9998", input.NewTcpListener(9998, input.TcpListenerOptions{}))
	p.Input("udp/9999", input.UdpListener(9999, input.UdpListenerOptions{}))
	p.Output("stdout/kv", output.StdOut(output.StdoutOptions{}))
	p.Output("stdout/json", output.StdOut(output.StdoutOptions{
		Codec: codec.Json(),
	}))

	if err := p.Run(); err != nil {
		slog.Error(err.Error())
	}
}

func setupLogging() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			//Level: slog.LevelDebug,
			Level:      slog.LevelInfo,
			TimeFormat: time.Kitchen,
		}),
	))
}
