package main

import (
	"fmt"
	"github.com/lmittmann/tint"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/input"
	"github.com/nicwaller/loglang/output"
	"log/slog"
	"os"
	"os/signal"
	"time"
)

func main() {
	setupLogging()

	p := loglang.NewPipeline("demo", loglang.PipelineOptions{
		MarkIngestionTime: false,
		Schema:            loglang.SchemaECS,
	})

	p.Input("heartbeat", input.Heartbeat(input.HeartbeatOptions{Interval: time.Second}))
	p.Input("tcp/9998", input.NewTcpListener(9998, input.TcpListenerOptions{}))
	p.Input("udp/9999", input.UdpListener(9999, input.UdpListenerOptions{}))
	p.Output("stdout/kv", output.StdOut(output.StdoutOptions{}))

	p.Filter("delay", func(event *loglang.Event, inject chan<- loglang.Event, drop func()) error {
		time.Sleep(400 * time.Millisecond)
		return nil
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			fmt.Println() // ^C appears in terminal, and I want a newline after that to keep things clean
			slog.Info("Caught Ctrl-C SIGINT")
			p.Stop()
		}
	}()

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
