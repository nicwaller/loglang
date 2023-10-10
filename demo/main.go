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

	//p.Input("heartbeat", input.Heartbeat(input.HeartbeatOptions{Interval: 2 * time.Second}))
	//p.Input("tcp/9998", input.NewTcpListener(9998, input.TcpListenerOptions{}))
	//p.Input("udp/9999", input.UdpListener(9999, input.UdpListenerOptions{
	//	Codec: codec.Json(),
	//}))
	p.Input("http", input.HttpListener(2000, input.HttpListenerOptions{
		//ReplyImmediately: true,
		//Codec:   codec.Json(),
		//Schema:  loglang.SchemaLogstashECS,
	}))
	p.Output("stdout/kv", output.StdOut(output.StdoutOptions{}))
	p.Output("slack", output.Slack(output.SlackOptions{
		BotToken:        os.Getenv("BOT_TOKEN"),
		FallbackChannel: "test-3",
		DetailFields:    true,
	}))

	//p.Filter("delay", func(event *loglang.Event, inject chan<- loglang.Event, drop func()) error {
	//	time.Sleep(1400 * time.Millisecond)
	//	return nil
	//})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fmt.Println() // ^C appears in terminal, and I want a newline after that to keep things clean
		slog.Info("Caught Ctrl-C SIGINT")
		p.Stop()
	}()

	if err := p.Run(); err != nil {
		slog.Error(err.Error())
	}
}

func setupLogging() {
	// TODO: can we automatically switch to debug output when certain errors occur?
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level: slog.LevelDebug,
			//Level:      slog.LevelInfo,
			TimeFormat: time.Kitchen,
		}),
	))
}
